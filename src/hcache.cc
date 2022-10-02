#include "cds/init.h"
#include "io_buffer.h"
#include "rapidjson/stringbuffer.h"
#include "rpc.h"
#include "simdjson/common_defs.h"
#include "storage.h"
#include "threading.h"
#include <algorithm>
#include <array>
#include <bits/types/struct_iovec.h>
#include <bitset>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <csignal>
#include <cstdint>
#include <exception>
#include <fcntl.h>
#include <fmt/format.h>
#include <liburing/io_uring.h>
#include <linux/time_types.h>
#include <memory>
#include <netinet/tcp.h>
#include <picohttpparser/picohttpparser.h>
#include <queue>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include <simdjson.h>
#include <string>
#include <string_view>
#include <strings.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <uringpp/event_loop.h>
#include <uringpp/file.h>
#include <uringpp/listener.h>
#include <uringpp/socket.h>
#include <uringpp/task.h>
#include <vector>
#include <zstd.h>

using uringpp::task;

struct send_conn_state {
  std::unique_ptr<uringpp::socket> conn;
};

task<void> rpc_reply_recv_loop(uringpp::socket &rpc_conn);

struct conn_pool {
  static constexpr size_t kMaxConns = 256;
  std::shared_ptr<loop_with_queue> loop;
  std::queue<std::coroutine_handle<>> waiters;
  std::string host;
  std::string port;
  std::queue<send_conn_state *> conns;
  size_t nr_conns = 0;
  struct conn_awaitable {
    conn_pool &pool_;
    conn_awaitable(conn_pool &pool) : pool_(pool) {}
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      pool_.waiters.push(h);
    }
    send_conn_state *await_resume() const noexcept {
      auto conn = pool_.conns.front();
      pool_.conns.pop();
      return conn;
    }
  };

  task<send_conn_state *> new_conn() {

    auto rpc_conn = co_await uringpp::socket::connect(loop, host, port);
    {
      int flags = 1;
      setsockopt(rpc_conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags,
                 sizeof(flags));
    }
    auto state = new send_conn_state{
        std::make_unique<uringpp::socket>(std::move(rpc_conn))};

    rpc_reply_recv_loop(*state->conn).detach();
    co_return state;
  }

  task<send_conn_state *> get_conn() {
    if (conns.empty()) {
      if (nr_conns < kMaxConns) {
        nr_conns++;
        co_return co_await new_conn();
      } else {
        co_return co_await conn_awaitable(*this);
      }
    }
    auto conn = conns.front();
    conns.pop();
    co_return conn;
  }
  void put_conn(send_conn_state *conn) {
    conns.push(conn);
    if (waiters.size()) {
      auto h = waiters.front();
      waiters.pop();
      h.resume();
    }
  }
};

std::vector<std::shared_ptr<loop_with_queue>> loops;
std::vector<std::shared_ptr<loop_with_queue>> rpc_loops;
std::vector<std::vector<conn_pool>> rpc_clients;
std::atomic<size_t> loop_started = 0;
std::atomic<size_t> clients_connected = 0;
std::unique_ptr<storage> store;
std::shared_ptr<loop_with_queue> main_loop;
std::vector<std::string> peer_hosts;
std::atomic<bool> peers_updated = false;
std::atomic<bool> rpc_connected = false;
std::vector<int> cores;
size_t nr_peers = 1;
size_t me = 0;

kv_cuckoo_set remote_cache;
fifo_queue evict_queue;
std::atomic<size_t> cached_count = 0;
constexpr size_t kMaxCacheCount = 1024 * 1024 * 4;

size_t get_shard(std::string_view key) {
  auto hash = XXH64(key.data(), key.size(), 19260817);
  return hash % nr_peers;
}

size_t itohex(char *buf, size_t n) {
  size_t i = 0;
  if (n == 0) {
    buf[i++] = '0';
    return i;
  }
  while (n) {
    auto c = n % 16;
    if (c < 10) {
      buf[i++] = '0' + c;
    } else {
      buf[i++] = 'a' + c - 10;
    }
    n /= 16;
  }
  std::reverse(buf, buf + i);
  return i;
}

const char HTTP_404[] = "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n";
const char HTTP_400[] = "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n";
const char OK_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
const char EMPTY_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
const char EMPTY_ARRAY[] = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n[]";
const char LOADING_RESPONSE[] =
    "HTTP/1.1 200 OK\r\ncontent-length: 7\r\n\r\nloading";
const char HTTP_200_HEADER[] = "HTTP/1.1 200 OK\r\ncontent-length: ";
const char CHUNK_RESPONSE[] =
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n[\r\n";
const char CHUNK_END[] = "1\r\n]\r\n0\r\n\r\n";
const char HEADER_END[] = "\r\n\r\n";

constexpr size_t kMaxWriteSize = 1024 * 1024 * 1024;

size_t itoa(char *buf, size_t n) {
  size_t i = 0;
  if (n == 0) {
    buf[i++] = '0';
    return i;
  }
  while (n) {
    buf[i++] = n % 10 + '0';
    n /= 10;
  }
  std::reverse(buf, buf + i);
  return i;
}

constexpr int kStateRecvHeader = 0;
constexpr int kStateRecvBody = 1;
constexpr int kStateProcess = 2;
task<void> send_all(uringpp::socket &conn, const char *data, size_t size) {
  while (size > 0) {
    auto n = co_await conn.send(data, size, MSG_NOSIGNAL);
    if (n <= 0) [[unlikely]] {
      if (n < 0) {
        fmt::print("send error: {}\n", strerror(-n));
      }
      fmt::print("send loop exit\n");
      co_return;
    }
    data += n;
    size -= n;
  }
}

uringpp::task<void> writev_all(uringpp::socket &conn, struct iovec *iov,
                               size_t iovcnt) {
  size_t total = 0;
  for (size_t i = 0; i < iovcnt; i++) {
    total += iov[i].iov_len;
  }
  size_t written = 0;
  while (written < total) {
    auto n = co_await conn.writev(iov, iovcnt);
    if (n <= 0) [[unlikely]] {
      break;
    }
    written += n;
    while (n > 0) {
      if (n >= iov[0].iov_len) {
        n -= iov[0].iov_len;
        iov++;
        iovcnt--;
      } else {
        iov[0].iov_base = (char *)iov[0].iov_base + n;
        iov[0].iov_len -= n;
        n = 0;
      }
    }
  }
}

[[nodiscard]] uringpp::task<void> send_json(uringpp::socket &conn,
                                            rapidjson::StringBuffer buffer) {
  {
    char length[8];
    size_t len_chars = itoa(length, buffer.GetSize());
    auto header_size =
        sizeof(HTTP_200_HEADER) + len_chars + sizeof(HEADER_END) - 2;
    struct iovec iov[4];
    iov[0].iov_base = const_cast<char *>(HTTP_200_HEADER);
    iov[0].iov_len = sizeof(HTTP_200_HEADER) - 1;
    iov[1].iov_base = length;
    iov[1].iov_len = len_chars;
    iov[2].iov_base = const_cast<char *>(HEADER_END);
    iov[2].iov_len = sizeof(HEADER_END) - 1;
    iov[3].iov_base = (void *)buffer.GetString();
    iov[3].iov_len = buffer.GetSize();
    co_await writev_all(conn, iov, 4);
  }
}

[[nodiscard]] uringpp::task<void> send_text(uringpp::socket &conn, char *data,
                                            size_t len) {

  char length[8];
  size_t len_chars = itoa(length, len);
  struct iovec iov[4];
  iov[0].iov_base = const_cast<char *>(HTTP_200_HEADER);
  iov[0].iov_len = sizeof(HTTP_200_HEADER) - 1;
  iov[1].iov_base = length;
  iov[1].iov_len = len_chars;
  iov[2].iov_base = const_cast<char *>(HEADER_END);
  iov[2].iov_len = sizeof(HEADER_END) - 1;
  iov[3].iov_base = data;
  iov[3].iov_len = len;
  co_await writev_all(conn, iov, 4);
}

uringpp::task<void> send_chunks(uringpp::socket &conn,
                                rapidjson::StringBuffer buffers[],
                                size_t count) {
  char chunk_length_str[16];
  char chunk_end[] = "\r\n";
  size_t chunk_length = 0;
  std::vector<struct iovec> iov(count + 2);
  for (size_t i = 0; i < count; i++) {
    iov[i + 1] = {(void *)buffers[i].GetString(), buffers[i].GetSize()};
    chunk_length += buffers[i].GetSize();
  }
  iov[0].iov_base = chunk_length_str;
  iov[0].iov_len = itohex(chunk_length_str, chunk_length);
  chunk_length_str[iov[0].iov_len++] = '\r';
  chunk_length_str[iov[0].iov_len++] = '\n';
  iov[count + 1].iov_base = chunk_end;
  iov[count + 1].iov_len = 2;
  co_await writev_all(conn, iov.data(), iov.size());
}

constexpr size_t kRPCReplyHeaderSize =
    sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);

uringpp::task<void> rpc_reply_empty(uringpp::socket &conn, uint64_t req_id) {
  char buf[kRPCReplyHeaderSize];
  *(uint64_t *)buf = req_id;
  *(uint32_t *)(buf + sizeof(uint64_t)) = 0;
  *(uint32_t *)(buf + sizeof(uint64_t) + sizeof(uint32_t)) = 0;
  co_await send_all(conn, buf, kRPCReplyHeaderSize);
}

uringpp::task<void> rpc_reply_simple(uringpp::socket &conn, uint64_t req_id,
                                     char *data, uint32_t len) {
  char header[kRPCReplyHeaderSize + sizeof(uint32_t)];
  auto p = header;
  *(uint64_t *)p = req_id;
  p += sizeof(uint64_t);
  *(uint32_t *)p = len + sizeof(uint32_t);
  p += sizeof(uint32_t);
  *(uint32_t *)p = len + sizeof(uint32_t);
  p += sizeof(uint32_t);
  *(uint32_t *)p = len;
  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = kRPCReplyHeaderSize + sizeof(uint32_t);
  iov[1].iov_base = data;
  iov[1].iov_len = len;
  co_await writev_all(conn, iov, 2);
}

uringpp::task<void> rpc_reply(uringpp::socket &conn, uint64_t req_id,
                              std::vector<char> data,
                              bool reply_compressed = false) {
  char header[kRPCReplyHeaderSize];
  auto len = static_cast<uint32_t>(data.size());
  auto p = header;
  *(uint64_t *)p = req_id;
  p += sizeof(uint64_t);
  *(uint32_t *)p = len;
  p += sizeof(uint32_t);
  auto body_pointer = &data[0];
  auto [compressed, compressed_len] = [&]() {
    if (!reply_compressed) {
      return std::make_pair(std::vector<char>(), len);
    }
    std::vector<char> compressed(ZSTD_compressBound(data.size()));
    uint32_t compressed_len = ZSTD_compress(
        compressed.data(), compressed.size(), data.data(), data.size(), 1);
    data.clear();
    return std::make_pair(std::move(compressed), compressed_len);
  }();
  if (!compressed.empty()) {
    body_pointer = compressed.data();
  }
  *(uint32_t *)p = compressed_len;
  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = kRPCReplyHeaderSize;
  iov[1].iov_base = body_pointer;
  iov[1].iov_len = compressed_len;
  co_await writev_all(conn, iov, 2);
}

constexpr size_t kRPCRequestHeaderSize =
    sizeof(uint64_t) + sizeof(method) + sizeof(uint32_t) + sizeof(uint32_t);
bool remove_cache_entry(std::string_view key) {
  if (auto p = remote_cache.erase(key); p) {
    mi_disposer<key_value_intl>()(p);
    cached_count.fetch_sub(1, std::memory_order_relaxed);
    return true;
  }
  return false;
}

void evict_cache() {
  while (!evict_queue.empty()) {
    std::string key;
    evict_queue.pop(key);
    if (remove_cache_entry(key)) {
      return;
    }
  }
}

bool update_cache(std::string_view key, std::string_view value) {
  return remote_cache.find(key, [value](auto &kv, ...) {
    kv.value.assign(value.begin(), value.end());
  });
}

void insert_cache(std::string_view key, std::string_view value) {
  if (update_cache(key, value)) {
    return;
  }
  key_value_intl *kv = new key_value_intl(key, value);
  auto [success, is_new] =
      remote_cache.update(*kv, [](bool inserted, auto &old_kv, auto &new_kv) {
        if (!inserted) {
          old_kv.value = new_kv.value;
        }
      });
  if (!is_new) {
    mi_disposer<key_value_intl>()(kv);
  } else {
    evict_queue.enqueue(kv->key);
    if (cached_count.fetch_add(1, std::memory_order_relaxed) >=
        kMaxCacheCount) {
      evict_cache();
    }
  }
}

task<void> handle_rpc(uringpp::socket conn, size_t conn_id) {
  auto loop = rpc_loops[conn_id % rpc_loops.size()];
  co_await loop->switch_to_io_thread();
  io_buffer request(65536);
  int state = kStateRecvHeader;
  uint64_t req_id = 0;
  uint32_t req_size = 0;
  uint32_t compressed_size = 0;
  method m = method::query;
  try {
    while (true) {
      if (state == kStateRecvHeader) {
        if (request.readable() >= kRPCRequestHeaderSize) {
          auto p = request.read_data();
          req_id = *reinterpret_cast<uint64_t *>(p);
          p += sizeof(req_id);
          m = *reinterpret_cast<method *>(p);
          p += sizeof(m);
          req_size = *reinterpret_cast<uint32_t *>(p);
          p += sizeof(req_size);
          compressed_size = *reinterpret_cast<uint32_t *>(p);
          p += sizeof(compressed_size);
          request.advance_read(kRPCRequestHeaderSize);
          state = kStateRecvBody;
          if (compressed_size > request.writable()) {
            request.expand(compressed_size - request.writable());
          }
        } else if (request.writable() < kRPCRequestHeaderSize) {
          request.expand(kRPCRequestHeaderSize - request.writable());
        }
      }
      if (state == kStateRecvBody) {
        if (request.readable() >= compressed_size) {
          state = kStateProcess;
        }
      }
      if (state != kStateProcess) {
        auto n = co_await conn.recv(request.write_data(), request.writable());
        if (n <= 0) [[unlikely]] {
          fmt::print("recv error: {} writable {}\n", n, request.writable());
          co_return;
        }
        request.advance_write(n);
        continue;
      }
      auto req_p = request.read_data();
      auto [decompressed, decompressed_size] = [&]() {
        if (m != method::batch && m != method::list && m != method::cbatch) {
          return std::make_pair(std::vector<char>(), req_size);
        }
        std::vector<char> decompressed(req_size);
        uint32_t decompressed_size =
            ZSTD_decompress(decompressed.data(), req_size, request.read_data(),
                            compressed_size);
        return std::make_pair(std::move(decompressed), decompressed_size);
      }();
      if (!decompressed.empty()) {
        req_p = decompressed.data();
        assert(decompressed_size == req_size);
      }
      switch (m) {
      case method::query: {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        auto value = store->query(key);
        if (value.has_value()) [[likely]] {
          co_await rpc_reply_simple(conn, req_id, value->data(), value->size());
        } else {
          co_await rpc_reply_empty(conn, req_id);
        }
      } break;
      case method::add: {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(value_size);
        std::string_view value(req_p, value_size);
        req_p += value_size;
        bool success = store->add(key, value);
        if (!success) {
          co_await rpc_reply_simple(conn, req_id, (char *)&value[0], 0);
        } else {
          co_await rpc_reply_empty(conn, req_id);
        }
      } break;
      case method::cadd: {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(value_size);
        std::string_view value(req_p, value_size);
        req_p += value_size;
        update_cache(key, value);
        co_await rpc_reply_empty(conn, req_id);
      } break;
      case method::del: {
        auto t = rpc_reply_empty(conn, req_id);
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        store->del(key);
        co_await t;
      } break;
      case method::cdel: {
        auto t = rpc_reply_empty(conn, req_id);
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        remove_cache_entry(key);
        co_await t;
      } break;
      case method::list: {
        uint32_t key_count = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_count);
        std::vector<std::string_view> keys;
        for (uint32_t i = 0; i < key_count; i++) {
          uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
          req_p += sizeof(key_size);
          std::string_view key(req_p, key_size);
          req_p += key_size;
          keys.push_back(key);
        }
        auto values = store->list(keys.begin(), keys.end());
        size_t rep_body_size = sizeof(uint32_t);
        for (auto &v : values) {
          rep_body_size += sizeof(uint32_t) * 2 + v.key.size() + v.value.size();
        }
        auto rep_body = std::vector<char>(rep_body_size);
        auto p = &rep_body[0];
        *reinterpret_cast<uint32_t *>(p) = values.size();
        p += sizeof(uint32_t);
        for (auto &v : values) {
          *reinterpret_cast<uint32_t *>(p) = v.key.size();
          p += sizeof(uint32_t);
          std::copy_n(v.key.data(), v.key.size(), p);
          p += v.key.size();
          *reinterpret_cast<uint32_t *>(p) = v.value.size();
          p += sizeof(uint32_t);
          std::copy_n(v.value.data(), v.value.size(), p);
          p += v.value.size();
        }
        co_await rpc_reply(conn, req_id, std::move(rep_body), true);
      } break;
      case method::batch: {
        auto t = rpc_reply_empty(conn, req_id);
        uint32_t count = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(count);
        for (uint32_t i = 0; i < count; i++) {
          uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
          req_p += sizeof(key_size);
          std::string_view key(req_p, key_size);
          req_p += key_size;
          uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
          req_p += sizeof(value_size);
          std::string_view value(req_p, value_size);
          req_p += value_size;
          store->add(key, value);
        }
        co_await t;
      } break;
      case method::cbatch: {
        auto t = rpc_reply_empty(conn, req_id);
        uint32_t count = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(count);
        for (uint32_t i = 0; i < count; i++) {
          uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
          req_p += sizeof(key_size);
          std::string_view key(req_p, key_size);
          req_p += key_size;
          uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
          req_p += sizeof(value_size);
          std::string_view value(req_p, value_size);
          req_p += value_size;
          update_cache(key, value);
        }
        co_await t;
      } break;
      case method::zadd: {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(value_size);
        std::string_view value(req_p, value_size);
        req_p += value_size;
        uint32_t score = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(score);
        store->zadd(key, value, score);
        co_await rpc_reply_empty(conn, req_id);
      } break;
      case method::zrmv: {
        auto t = rpc_reply_empty(conn, req_id);
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(value_size);
        std::string_view value(req_p, value_size);
        req_p += value_size;
        store->zrmv(key, value);
        co_await t;
      } break;
      case method::zrange: {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t min_score = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(min_score);
        uint32_t max_score = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(max_score);
        auto values = store->zrange(key, min_score, max_score);
        if (!values.has_value()) {
          co_await rpc_reply(conn, req_id, {});
        } else {
          auto const &score_values = values.value();
          size_t rep_body_size = sizeof(uint32_t);
          for (auto &v : score_values) {
            rep_body_size += sizeof(uint32_t) * 2 + v.value.size();
          }
          auto rep_body = std::vector<char>(rep_body_size);
          auto p = &rep_body[0];
          *reinterpret_cast<uint32_t *>(p) = score_values.size();
          p += sizeof(uint32_t);
          for (auto &sv : score_values) {
            *reinterpret_cast<uint32_t *>(p) = sv.value.size();
            p += sizeof(uint32_t);
            std::copy_n(sv.value.data(), sv.value.size(), p);
            p += sv.value.size();
            *reinterpret_cast<uint32_t *>(p) = sv.score;
            p += sizeof(uint32_t);
          }
          co_await rpc_reply(conn, req_id, std::move(rep_body), true);
        }
      } break;
      default: {
        co_await rpc_reply_empty(conn, req_id);
      }
      }
      request.advance_read(compressed_size);
      state = kStateRecvHeader;
      req_size = 0;
      compressed_size = 0;
    }
  } catch (std::exception const &e) {
    fmt::print("RPC server error: {}\n", e.what());
  }
}

struct pending_call {
  std::coroutine_handle<> h;
  std::vector<char> response;
  bool reply_compressed = false;
};

task<std::vector<char>> send_rpc_request(conn_pool &pool,
                                         std::vector<char> body,
                                         pending_call *pending, method m) {
  uint32_t req_size = body.size();
  char *body_pointer = &body[0];
  auto [compressed, compressed_len] = [&]() {
    if (m != method::list && m != method::batch && m != method::cbatch) {
      return std::make_pair(std::vector<char>(), req_size);
    }
    std::vector<char> compressed(m == method::batch || m == method::list ||
                                         m == method::cbatch
                                     ? ZSTD_compressBound(body.size())
                                     : 0);
    uint32_t compressed_len = ZSTD_compress(&compressed[0], compressed.size(),
                                            &body[0], body.size(), 1);
    if (ZSTD_isError(compressed_len)) {
      fmt::print(stderr, "ZSTD_compress failed: {}\n",
                 ZSTD_getErrorName(compressed_len));
    }
    body.clear();
    return std::make_pair(std::move(compressed), compressed_len);
  }();
  if (!compressed.empty()) {
    body_pointer = &compressed[0];
  }
  char header[kRPCRequestHeaderSize];
  auto p = &header[0];
  *reinterpret_cast<uint64_t *>(p) = reinterpret_cast<uint64_t>(pending);
  p += sizeof(uint64_t);
  *reinterpret_cast<method *>(p) = m;
  p += sizeof(method);
  *reinterpret_cast<uint32_t *>(p) = req_size;
  p += sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(p) = compressed_len;
  p += sizeof(uint32_t);
  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = kRPCRequestHeaderSize;
  iov[1].iov_base = body_pointer;
  iov[1].iov_len = compressed_len;
  auto conn = co_await pool.get_conn();
  co_await writev_all(*conn->conn, iov, 2);
  pool.put_conn(conn);
  co_await std::suspend_always{};
  co_return std::move(pending->response);
}

task<std::vector<char>> rpc_call(conn_pool &pool, method m,
                                 std::vector<char> body,
                                 bool reply_compressed = false) {
  auto pending = new pending_call;
  pending->reply_compressed = reply_compressed;
  auto t = send_rpc_request(pool, std::move(body), pending, m);
  pending->h = t.h_;
  return t;
}

task<std::optional<std::pair<std::vector<char>, std::string_view>>>
remote_query(conn_pool &pool, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto body = std::vector<char>(body_size);
  *reinterpret_cast<uint32_t *>(&body[0]) = key.size();
  std::copy_n(key.data(), key.size(), body.begin() + sizeof(uint32_t));
  auto response = co_await rpc_call(pool, method::query, std::move(body));
  if (response.empty()) {
    co_return std::nullopt;
  }
  auto p = response.data();
  auto value_size = *reinterpret_cast<uint32_t *>(p);
  p += sizeof(value_size);
  co_return std::make_pair(std::move(response),
                           std::string_view(p, value_size));
}

task<bool> remote_add(conn_pool &pool, std::string_view key,
                      std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  auto response = co_await rpc_call(pool, method::add, std::move(body));
  bool success = response.empty();
  co_return success;
}

task<bool> remote_cadd(conn_pool &pool, std::string_view key,
                       std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  auto response = co_await rpc_call(pool, method::cadd, std::move(body));
  bool success = response.empty();
  co_return success;
}

task<void> remote_del(conn_pool &pool, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  co_await rpc_call(pool, method::del, std::move(body));
}

task<void> remote_cdel(conn_pool &pool, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  co_await rpc_call(pool, method::cdel, std::move(body));
}

task<void> remote_batch(
    conn_pool &pool,
    std::unordered_map<std::string_view, std::string_view> const &kvs) {
  auto body_size = sizeof(uint32_t);
  for (auto const kv : kvs) {
    body_size += sizeof(uint32_t) * 2 + kv.first.size() + kv.second.size();
  }
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = kvs.size();
  body_p += sizeof(uint32_t);
  for (auto const kv : kvs) {
    *reinterpret_cast<uint32_t *>(body_p) = kv.first.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.first.data(), kv.first.size(), body_p);
    body_p += kv.first.size();
    *reinterpret_cast<uint32_t *>(body_p) = kv.second.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.second.data(), kv.second.size(), body_p);
    body_p += kv.second.size();
  }
  co_await rpc_call(pool, method::batch, std::move(body));
}

task<void> remote_cbatch(
    conn_pool &pool,
    std::unordered_map<std::string_view, std::string_view> const &kvs) {
  auto body_size = sizeof(uint32_t);
  for (auto const kv : kvs) {
    body_size += sizeof(uint32_t) * 2 + kv.first.size() + kv.second.size();
  }
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = kvs.size();
  body_p += sizeof(uint32_t);
  for (auto const kv : kvs) {
    *reinterpret_cast<uint32_t *>(body_p) = kv.first.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.first.data(), kv.first.size(), body_p);
    body_p += kv.first.size();
    *reinterpret_cast<uint32_t *>(body_p) = kv.second.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.second.data(), kv.second.size(), body_p);
    body_p += kv.second.size();
  }
  co_await rpc_call(pool, method::cbatch, std::move(body));
}

task<std::pair<std::vector<char>, std::vector<key_value_view>>>
remote_list(conn_pool &pool, std::unordered_set<std::string_view> const &keys) {
  auto body_size = sizeof(uint32_t);
  for (auto const &key : keys) {
    body_size += sizeof(uint32_t) + key.size();
  }
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = keys.size();
  body_p += sizeof(uint32_t);
  for (auto const &key : keys) {
    *reinterpret_cast<uint32_t *>(body_p) = key.size();
    body_p += sizeof(uint32_t);
    std::copy_n(key.data(), key.size(), body_p);
    body_p += key.size();
  }
  auto response = co_await rpc_call(pool, method::list, std::move(body), true);
  auto rep_p = response.data();
  auto rep_count = *reinterpret_cast<uint32_t *>(rep_p);
  rep_p += sizeof(uint32_t);
  std::vector<key_value_view> kvs;
  for (auto i = 0; i < rep_count; ++i) {
    auto key_size = *reinterpret_cast<uint32_t *>(rep_p);
    rep_p += sizeof(uint32_t);
    auto key = std::string_view(rep_p, key_size);
    rep_p += key_size;
    auto value_size = *reinterpret_cast<uint32_t *>(rep_p);
    rep_p += sizeof(uint32_t);
    auto value = std::string_view(rep_p, value_size);
    rep_p += value_size;
    kvs.emplace_back(key, value);
  }
  co_return {std::move(response), std::move(kvs)};
}

task<bool> remote_zadd(conn_pool &pool, std::string_view key,
                       std::string_view value, uint32_t score) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 3;
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  body_p += value.size();
  *reinterpret_cast<uint32_t *>(body_p) = score;
  auto response = co_await rpc_call(pool, method::zadd, std::move(body));
  bool success = response.empty();
  co_return success;
}

task<std::optional<std::pair<std::vector<char>, std::vector<score_value_view>>>>
remote_zrange(conn_pool &pool, std::string_view key, uint32_t min_score,
              uint32_t max_score) {
  auto body_size = sizeof(uint32_t) * 3 + key.size();
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = min_score;
  body_p += sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(body_p) = max_score;
  auto response =
      co_await rpc_call(pool, method::zrange, std::move(body), true);
  if (response.empty()) {
    co_return std::nullopt;
  }
  auto rep_p = response.data();
  auto rep_count = *reinterpret_cast<uint32_t *>(rep_p);
  rep_p += sizeof(uint32_t);
  std::vector<score_value_view> sv;
  for (auto i = 0; i < rep_count; ++i) {
    auto value_size = *reinterpret_cast<uint32_t *>(rep_p);
    rep_p += sizeof(uint32_t);
    auto value = std::string_view(rep_p, value_size);
    rep_p += value_size;
    auto score = *reinterpret_cast<uint32_t *>(rep_p);
    rep_p += sizeof(uint32_t);
    sv.emplace_back(value, score);
  }
  co_return std::make_pair(std::move(response), std::move(sv));
}

task<void> remote_zrmv(conn_pool &pool, std::string_view key,
                       std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto body = std::vector<char>(body_size);
  auto body_p = &body[0];
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(pool, method::zrmv, std::move(body));
}

task<void> rpc_reply_recv_loop(uringpp::socket &rpc_conn) {
  io_buffer response(65536);
  int state = kStateRecvHeader;
  uint32_t rep_size = 0;
  uint64_t req_id = 0;
  uint32_t compressed_size = 0;
  while (true) {
    if (state == kStateRecvHeader) {
      if (response.readable() >= kRPCReplyHeaderSize) {
        auto p = response.read_data();
        req_id = *reinterpret_cast<uint64_t *>(p);
        p += sizeof(req_id);
        rep_size = *reinterpret_cast<uint32_t *>(p);
        p += sizeof(rep_size);
        compressed_size = *reinterpret_cast<uint32_t *>(p);
        p += sizeof(compressed_size);
        state = kStateRecvBody;
        response.advance_read(kRPCReplyHeaderSize);
        if (response.writable() < compressed_size) {
          response.expand(compressed_size - response.writable());
        }
      } else if (response.writable() < kRPCReplyHeaderSize) {
        response.expand(kRPCReplyHeaderSize - response.writable());
      }
    }
    if (state == kStateRecvBody) {
      if (response.readable() >= compressed_size) {
        state = kStateProcess;
      }
    }
    if (state != kStateProcess) {
      assert(response.writable() > 0);
      auto n = co_await rpc_conn.recv(response.write_data(),
                                      response.writable(), MSG_NOSIGNAL);
      if (n <= 0) {
        if (n < 0) {
          fmt::print("recv error: {}\n", strerror(-n));
        }
        fmt::print("recv loop exit\n");
        co_return;
      }
      response.advance_write(n);
      continue;
    }
    auto pending = reinterpret_cast<pending_call *>(req_id);
    pending->response.resize(rep_size);
    if (pending->reply_compressed && rep_size > 0) {
      assert(compressed_size > 0);
      auto decompressed_size =
          ZSTD_decompress(&pending->response[0], rep_size, response.read_data(),
                          compressed_size);
      assert(decompressed_size == rep_size);
      response.advance_read(compressed_size);
    } else {
      pending->response.assign(response.read_data(),
                               response.read_data() + rep_size);
      response.advance_read(rep_size);
    }
    pending->h.resume();
    delete pending;
    state = kStateRecvHeader;
    rep_size = 0;
    compressed_size = 0;
  }
}

task<void> connect_rpc_client(std::string port);

task<void> rpc_server(std::shared_ptr<loop_with_queue> loop, std::string port) {
  size_t rpc_conn_id = 0;
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", port);
  fmt::print("Starting RPC server\n");
  connect_rpc_client(port).detach();
  while (true) {
    auto [addr, conn] =
        co_await listener.accept(rpc_loops[rpc_conn_id % rpc_loops.size()]);
    {
      int flags = 1;
      setsockopt(conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags,
                 sizeof(flags));
    }
    handle_rpc(std::move(conn), rpc_conn_id++).detach();
  }
  co_return;
}

template <class It>
task<void> send_score_values(uringpp::socket &conn, size_t count, It begin,
                             It end) {
  if (count < 1000000) {
    // zrange
    rapidjson::StringBuffer buffer;
    auto d = rapidjson::Document();
    auto &sv_list = d.SetArray();
    auto &allocator = d.GetAllocator();
    auto write_sv = [&](std::string_view value, uint32_t score) {
      auto sv_object = rapidjson::Value(rapidjson::kObjectType);
      auto v_string = rapidjson::StringRef(value.data(), value.size());
      sv_object.AddMember("value", v_string, allocator);
      sv_object.AddMember("score", score, allocator);
      sv_list.PushBack(sv_object, allocator);
    };
    for (auto it = begin; it != end; ++it) {
      write_sv(it->value, it->score);
    }
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);
    co_await send_json(conn, std::move(buffer));
  } else {
    co_await send_all(conn, CHUNK_RESPONSE, sizeof(CHUNK_RESPONSE) - 1);
    auto d = rapidjson::Document();
    auto &allocator = d.GetAllocator();
    constexpr size_t batch_size = 256;
    rapidjson::StringBuffer buffers[batch_size];
    size_t batch_idx = 0;
    bool is_first = true;
    auto write_sv = [&](std::string_view value, uint32_t score) -> task<void> {
      auto &sv_object = d.SetObject();
      auto v_string = rapidjson::StringRef(value.data(), value.size());
      sv_object.AddMember("value", v_string, allocator);
      sv_object.AddMember("score", score, allocator);
      auto &buffer = buffers[batch_idx];
      buffer.Clear();
      if (is_first) {
        is_first = false;
      } else {
        buffer.Put(',');
      }
      rapidjson::Writer writer(buffer);
      d.Accept(writer);
      if (++batch_idx == batch_size) {
        co_await send_chunks(conn, buffers, batch_idx);
        batch_idx = 0;
      }
    };
    for (auto it = begin; it != end; ++it) {
      co_await write_sv(it->value, it->score);
    }
    if (batch_idx > 0) {
      co_await send_chunks(conn, buffers, batch_idx);
    }
    co_await send_all(conn, CHUNK_END, sizeof(CHUNK_END) - 1);
  }
}

task<void> handle_http(uringpp::socket conn, size_t conn_id) {
  auto conn_shard = conn_id % loops.size();
  auto loop = loops[conn_shard];
  co_await loop->switch_to_io_thread();
  try {
    io_buffer request(65536);
    int read_idx = 0;
    bool receiving_body = false;
    int n;
    size_t body_received = 0;
    size_t content_length = 0;
    int parser_rc = -2;
    char *body_start;
    const char *path;
    const char *method;
    size_t method_offset;
    size_t path_offset;
    size_t header_offset;
    int minor_version;
    size_t path_len;
    size_t method_len;
    size_t num_headers = 16;
    bool sending = false;
    bool closed = false;
    phr_header headers[16];
    simdjson::dom::parser parser;
    while (true) {
      // Receiving Header
      if (parser_rc == -2) {
        if (request.readable()) {
          parser_rc = phr_parse_request(
              request.read_data(), request.readable(), &method, &method_len,
              &path, &path_len, &minor_version, headers, &num_headers, 0);
          if (parser_rc == -1) {
            co_await send_all(conn, HTTP_400, sizeof(HTTP_400) - 1);
            break;
          } else if (parser_rc > 0) {
            path_offset = path - request.read_data();
            method_offset = method - request.read_data();
            content_length = 0;
            for (int i = 0; i < num_headers; ++i) {
              if (::strncasecmp(headers[i].name, "content-length",
                                headers[i].name_len) == 0) {
                content_length = std::stoul(
                    std::string(headers[i].value, headers[i].value_len));
                break;
              }
            }
            if (content_length + simdjson::SIMDJSON_PADDING >
                request.writable()) {
              request.expand(content_length + simdjson::SIMDJSON_PADDING -
                             request.writable());
            }
          }
        } else if (request.writable() == 0) {
          request.expand(2048);
        }
      }

      if (request.writable() < simdjson::SIMDJSON_PADDING) {
        request.expand(simdjson::SIMDJSON_PADDING);
      }

      if (parser_rc > 0) {
        path = request.read_data() + path_offset;
        method = request.read_data() + method_offset;
        body_start = request.read_data() + parser_rc;
        body_received = request.readable() - parser_rc;
        receiving_body = body_received < content_length;
      }

      if (parser_rc == -2 || receiving_body || request.readable() == 0) {
        assert(request.writable() > 0);
        n = co_await conn.recv(request.write_data(), request.writable(),
                               MSG_NOSIGNAL);
        if (n <= 0) {
          co_return;
        }
        request.advance_write(n);
        continue;
      }

      switch (method[0]) {
      case 'G': {
        switch (path[1]) {
        default: {
          co_await send_all(conn, OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'i': {
          // init
          if (store->kv_loaded()) {
            co_await send_all(conn, OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
          } else {
            co_await send_all(conn, LOADING_RESPONSE,
                              sizeof(LOADING_RESPONSE) - 1);
          }
        } break;
        case 'q': {
          // query
          std::string_view key(path + 7, path_len - 7);
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            auto value = store->query(key);
            if (!value.has_value()) {
              co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              co_await send_text(conn, value->data(), value->size());
            }
          } else {
            std::string cached_value;
            auto cached =
                remote_cache.find(key, [&cached_value](auto &kv, ...) {
                  cached_value = kv.value;
                });
            if (cached) {
              co_await send_text(conn, cached_value.data(),
                                 cached_value.size());

            } else {
              auto value = co_await remote_query(
                  rpc_clients[key_shard][conn_shard], key);
              if (!value.has_value()) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
              } else {
                co_await send_text(conn, (char *)value->second.data(),
                                   value->second.size());
                insert_cache(key, value->second);
              }
            }
          }
        } break;
        case 'd': {
          // del
          std::string_view key(path + 5, path_len - 5);
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            store->del(key);
          } else {
            co_await remote_del(rpc_clients[key_shard][conn_shard], key);
            remove_cache_entry(key);
            for (size_t i = 0; i < nr_peers; ++i) {
              if (i == me || i == key_shard) {
                continue;
              }
              remote_cdel(rpc_clients[i][conn_shard], key).detach();
            }
          }
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'z': {
          // zrmv
          auto slash_ptr = path + 6;
          auto end_ptr = slash_ptr + path_len - 6;
          while (slash_ptr != end_ptr) {
            if (*(slash_ptr++) == '/') {
              break;
            }
          }
          auto const key = std::string_view(path + 6, slash_ptr - 1);
          auto const value = std::string_view(slash_ptr, end_ptr - slash_ptr);
          auto key_shard = get_shard(key);
          store->zrmv(key, value);
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          std::vector<task<void>> tasks;
          for (size_t i = 0; i < nr_peers; ++i) {
            if (i == me) {
              continue;
            }
            tasks.emplace_back(
                remote_zrmv(rpc_clients[i][conn_shard], key, value));
          }
          for (auto &t : tasks) {
            co_await t;
          }
        } break;
        }
      } break;
      case 'P': {
        switch (path[1]) {
        default: {
          co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
        } break;
        case 'u': {
          // updateCluster
          std::string_view clusterjson(body_start, body_received);
          simdjson::padded_string padded(clusterjson);
          auto doc = parser.parse(padded);
          nr_peers = doc["hosts"].get_array().size();
          me = doc["index"].get_uint64() - 1;
          auto f = co_await uringpp::file::open(loop, "/data/cluster.json",
                                                O_CREAT | O_RDWR, 0644);
          co_await f.write(body_start, content_length, 0);
          co_await f.close();
          {
            bool init = false;
            if (peers_updated.compare_exchange_strong(init, true)) {
              co_await connect_rpc_client("58080");
              co_await loop->switch_to_io_thread();
              fmt::print("RPC client connected\n");
            }
          }
          {
            bool init = false;
            if (store->kv_initializing_.compare_exchange_strong(init, true)) {
              auto init_thread = std::thread([]() {
                enable_on_cores(cores);
                store->first_time_init();
              });
              init_thread.detach();
            }
          }
          co_await send_all(conn, OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'a': {
          // add
          auto json = simdjson::padded_string_view(body_start, content_length,
                                                   request.capacity() -
                                                       request.read_idx());
          auto doc = parser.parse(json);
          auto const key = doc["key"].get_string().take_value();
          auto const value = doc["value"].get_string().take_value();
          auto key_shard = get_shard(key);
          bool success = false;
          if (key_shard == me) {
            success = store->add(key, value);
            co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          } else {
            success = co_await remote_add(rpc_clients[key_shard][conn_shard],
                                          key, value);
            insert_cache(key, value);
            co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
            for (size_t i = 0; i < nr_peers; ++i) {
              if (i == me || i == key_shard) {
                continue;
              }
              co_await remote_cadd(rpc_clients[i][conn_shard], key, value);
            }
          }
        } break;
        case 'b': {
          // batch
          auto json = simdjson::padded_string_view(body_start, content_length,
                                                   request.capacity() -
                                                       request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::vector<std::unordered_map<std::string_view, std::string_view>>
              sharded_keys(nr_peers);
          auto batch = store->start_batch();
          for (auto &&kv : arr) {
            auto const key = kv["key"].get_string().take_value();
            auto const value = kv["value"].get_string().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              store->add_batch(batch, key, value);
            } else {
              sharded_keys[key_shard][key] = value;
              insert_cache(key, value);
            }
          }
          std::vector<task<void>> tasks;
          std::vector<task<void>> cache_tasks;
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            tasks.emplace_back(std::move(remote_batch(
                rpc_clients[key_shard][conn_shard], sharded_keys[key_shard])));
          }
          store->commit_batch(batch);
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          for (auto &task : tasks) {
            co_await task;
          }
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            for (size_t i = 0; i < nr_peers; ++i) {
              if (i == me || i == key_shard) {
                continue;
              }
              cache_tasks.emplace_back(std::move(remote_cbatch(
                  rpc_clients[i][conn_shard], sharded_keys[key_shard])));
            }
          }
          for (auto &t : cache_tasks) {
            co_await t;
          }
        } break;
        case 'l': {
          // list
          auto json = simdjson::padded_string_view(body_start, content_length,
                                                   request.capacity() -
                                                       request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::unordered_set<std::string_view> keys;
          std::vector<std::unordered_set<std::string_view>> sharded_keys(
              nr_peers);
          std::vector<std::unordered_set<std::string_view>> cached_keys(
              nr_peers);
          std::vector<std::vector<key_value_view>> remote_key_values;
          std::vector<key_view_value> cached_key_values;
          for (auto &&k : arr) {
            auto key = k.get_string().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              keys.insert(key);
            } else {
              if (sharded_keys[key_shard].count(key) ||
                  cached_keys[key_shard].count(key)) {
                continue;
              }
              key_view_value cached_value;
              if (remote_cache.find(key, [&](auto &kv, ...) {
                    cached_value.key = kv.key;
                    cached_value.value = kv.value;
                  })) {
                cached_key_values.emplace_back(std::move(cached_value));
                cached_keys[key_shard].insert(key);
              } else {
                sharded_keys[key_shard].insert(key);
              }
            }
          }
          auto remote_kv_count = 0;
          std::vector<std::vector<char>> remote_buffers;
          std::vector<
              task<std::pair<std::vector<char>, std::vector<key_value_view>>>>
              tasks;
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            tasks.emplace_back(remote_list(rpc_clients[key_shard][conn_shard],
                                           sharded_keys[key_shard]));
          }
          auto local_key_values = store->list(keys.begin(), keys.end());
          for (auto &t : tasks) {
            auto remote_kvs = co_await t;
            remote_kv_count += remote_kvs.second.size();
            remote_buffers.emplace_back(std::move(remote_kvs.first));
            remote_key_values.emplace_back(std::move(remote_kvs.second));
          }
          if (local_key_values.empty() && cached_key_values.empty() &&
              remote_kv_count == 0) {
            co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            if (local_key_values.size() + remote_kv_count < 10000000) {
              rapidjson::StringBuffer buffer;
              {
                auto d = rapidjson::Document();
                auto &kv_list = d.SetArray();
                auto &allocator = d.GetAllocator();
                kv_list.Reserve(remote_kv_count + local_key_values.size(),
                                allocator);
                auto write_kv = [&](std::string_view key,
                                    std::string_view value) {
                  auto kv_object = rapidjson::Value(rapidjson::kObjectType);
                  auto k_string = rapidjson::StringRef(key.data(), key.size());
                  auto v_string =
                      rapidjson::StringRef(value.data(), value.size());
                  kv_object.AddMember("key", k_string, allocator);
                  kv_object.AddMember("value", v_string, allocator);
                  kv_list.PushBack(kv_object, allocator);
                };
                for (auto &kv : local_key_values) {
                  write_kv(kv.key, kv.value);
                }
                for (auto &kv : cached_key_values) {
                  write_kv(kv.key, kv.value);
                }
                for (auto const &remote_kvs : remote_key_values) {
                  for (auto const kv : remote_kvs) {
                    insert_cache(kv.key, kv.value);
                    write_kv(kv.key, kv.value);
                  }
                }
                rapidjson::Writer writer(buffer);
                d.Accept(writer);
              }
              co_await send_json(conn, std::move(buffer));
            } else {
              co_await send_all(conn, CHUNK_RESPONSE,
                                sizeof(CHUNK_RESPONSE) - 1);
              constexpr size_t batch_size = 256;
              size_t batch_idx = 0;
              bool is_first = true;
              rapidjson::StringBuffer buffers[batch_size];
              auto d = rapidjson::Document();
              auto &allocator = d.GetAllocator();
              auto write_kv = [&](std::string_view key,
                                  std::string_view value) -> task<void> {
                auto &kv_object = d.SetObject();
                auto k_string = rapidjson::StringRef(key.data(), key.size());
                auto v_string =
                    rapidjson::StringRef(value.data(), value.size());
                kv_object.AddMember("key", k_string, allocator);
                kv_object.AddMember("value", v_string, allocator);
                buffers[batch_idx].Clear();
                if (is_first) {
                  is_first = false;
                } else {
                  buffers[batch_idx].Put(',');
                }
                auto writer = rapidjson::Writer(buffers[batch_idx]);
                d.Accept(writer);
                if (++batch_idx == batch_size) {
                  co_await send_chunks(conn, buffers, batch_idx);
                  batch_idx = 0;
                }
              };
              for (auto &kv : local_key_values) {
                co_await write_kv(kv.key, kv.value);
              }
              for (auto &kv : cached_key_values) {
                co_await write_kv(kv.key, kv.value);
              }
              for (auto const &remote_kvs : remote_key_values) {
                for (auto const kv : remote_kvs) {
                  insert_cache(kv.key, kv.value);
                  co_await write_kv(kv.key, kv.value);
                }
              }
              if (batch_idx > 0) {
                co_await send_chunks(conn, buffers, batch_idx);
              }
              co_await send_all(conn, CHUNK_END, sizeof(CHUNK_END) - 1);
            }
          }
        } break;
        case 'z': {
          switch (path[2]) {
          case 'a': {
            // zadd
            std::string_view key(&path[6], path_len - 6);
            auto json = simdjson::padded_string_view(body_start, content_length,
                                                     request.capacity() -
                                                         request.read_idx());
            auto score_value = parser.parse(json);
            auto value = score_value["value"].get_string().take_value();
            auto score = score_value["score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            store->zadd(key, value, score);
            co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
            std::vector<task<bool>> tasks;
            for (size_t i = 0; i < nr_peers; i++) {
              if (i == me) {
                continue;
              }
              tasks.emplace_back(std::move(
                  remote_zadd(rpc_clients[i][conn_shard], key, value, score)));
            }
            for (auto &t : tasks) {
              co_await t;
            }
          } break;
          case 'r': {
            // zrange
            std::string_view key(&path[8], path_len - 8);
            auto json = simdjson::padded_string_view(body_start, content_length,
                                                     request.capacity() -
                                                         request.read_idx());
            auto score_range = parser.parse(json);
            auto min_score = score_range["min_score"].get_uint64().take_value();
            auto max_score = score_range["max_score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            if (true) {
              auto score_values = store->zrange(key, min_score, max_score);
              if (!score_values.has_value()) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
              } else {
                co_await send_score_values(conn, score_values->size(),
                                           score_values->begin(),
                                           score_values->end());
              }
            } else if (false) {
              auto score_values =
                  co_await remote_zrange(rpc_clients[key_shard][conn_shard],
                                         key, min_score, max_score);
              if (!score_values.has_value()) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
              } else {
                co_await send_score_values(conn, score_values->second.size(),
                                           score_values->second.begin(),
                                           score_values->second.end());
              }
            }
          } break;
          }
        } break;
        }
      } break;
      default:
        co_await send_all(conn, OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
      }
      request.advance_read(parser_rc + content_length);
      content_length = 0;
      body_received = 0;
      num_headers = 16;
      parser_rc = -2;
    }
  } catch (std::exception &e) {
    fmt::print("conn {} error {}\n", conn.fd(), e.what());
  }

  co_return;
}

task<void> http_server(std::shared_ptr<loop_with_queue> loop) {
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", "8080", 10000);
  size_t conn_id = 0;
  fmt::print("Starting HTTP server\n");
  while (true) {
    try {
      auto [addr, conn] =
          co_await listener.accept(loops[conn_id % loops.size()]);
      {
        int flags = 1;
        setsockopt(conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags,
                   sizeof(flags));
      }
      handle_http(std::move(conn), conn_id).detach();
      conn_id++;
    } catch (std::exception &e) {
      fmt::print("Failed to accept {}\n", e.what());
    }
  }
}

task<void> connect_rpc_client(std::string port) {
  (void)loop_started.load();
  try {
    co_await main_loop->switch_to_io_thread();
    fmt::print("Starting RPC client\n");
    auto f = co_await uringpp::file::open(main_loop, "/data/cluster.json",
                                          O_RDONLY, 0644);
    std::vector<char> buf;
    while (true) {
      std::array<char, 4096> read_buf;
      auto rc = co_await f.read(&read_buf[0], read_buf.size(), buf.size());
      if (rc <= 0) {
        break;
      }
      buf.insert(buf.end(), read_buf.begin(), read_buf.begin() + rc);
    }
    simdjson::dom::parser parser;
    simdjson::padded_string json =
        simdjson::padded_string(buf.data(), buf.size());
    auto doc = parser.parse(json);
    auto hosts = doc["hosts"].get_array().take_value();
    me = doc["index"].get_uint64().take_value() - 1;
    nr_peers = hosts.size();
    for (auto &&host : hosts) {
      peer_hosts.emplace_back(host.get_string().take_value());
    }
    rpc_clients.resize(hosts.size());
    peers_updated = true;
    for (size_t peer_idx = 0; peer_idx < nr_peers; ++peer_idx) {
      if (peer_idx == me) {
        continue;
      }
      auto const &host = peer_hosts[peer_idx];
      fmt::print("Connecting to {}:{}\n", host, port);
      rpc_clients[peer_idx].resize(loops.size());
      for (size_t lidx = 0; lidx < loops.size(); ++lidx) {
        rpc_clients[peer_idx][lidx].host = host;
        rpc_clients[peer_idx][lidx].port = port;
        rpc_clients[peer_idx][lidx].loop = loops[lidx];
      }
    }
    rpc_connected = true;
  } catch (std::exception &e) {
    fmt::print("Failed to connect to peers {}\n", e.what());
    peers_updated = false;
    co_return;
  }
}

void db_flusher() {
  std::thread([] {
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      store->flush();
    }
  }).detach();
}

int main(int argc, char *argv[]) {
  cds::Initialize();
  cds::gc::HP gchp;
  main_loop = loop_with_queue::create(4096);
  main_loop->waker().detach();
  ::signal(SIGPIPE, SIG_IGN);
  cores = get_cpu_affinity();
  loops.resize(cores.size());
  rpc_loops.resize(cores.size());
  store = std::make_unique<storage>();
  {
    auto kv_thread = std::thread([]() { store->load_kv(); });
    auto zset_thread = std::thread([]() { store->load_zset(); });
    kv_thread.join();
    zset_thread.join();
  }
  bind_cpu(cores[0]);
  loop_started.fetch_add(1);
  loops[0] = main_loop;
  rpc_loops[0] = main_loop;
  for (int i = 1; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i]] {
      cds::threading::Manager::attachThread();
      fmt::print("thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      auto loop = loop_with_queue::create(4096, IORING_SETUP_ATTACH_WQ,
                                          main_loop->fd());
      loops[i] = loop;
      rpc_loops[i] = loop;
      loop_started.fetch_add(1);
      loop->waker().detach();
      for (;;) {
        loop->poll();
      }
    });
    thread.detach();
  }
  while (loop_started.load() != cores.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  rpc_server(main_loop, "58080").detach();
  http_server(main_loop).detach();
  db_flusher();
  for (;;) {
    main_loop->poll();
  }
  return 0;
}
