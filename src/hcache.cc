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
#include <cassert>
#include <chrono>
#include <coroutine>
#include <csignal>
#include <cstdint>
#include <exception>
#include <fcntl.h>
#include <fmt/format.h>
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
#include <unordered_set>
#include <uringpp/event_loop.h>
#include <uringpp/file.h>
#include <uringpp/listener.h>
#include <uringpp/socket.h>
#include <uringpp/task.h>

using uringpp::task;

struct send_conn_state {
  std::unique_ptr<uringpp::socket> conn;
};

task<void> rpc_reply_recv_loop(uringpp::socket &rpc_conn);

struct conn_pool {
  static constexpr size_t kMaxConns = 2048;
  std::shared_ptr<loop_with_queue> loop;
  std::queue<std::coroutine_handle<>> waiters;
  std::string host;
  std::string port;
  std::queue<send_conn_state *> conns;
  size_t nr_conns = 0;
  struct conn_awaitable {
    conn_pool &pool_;
    conn_awaitable(conn_pool &pool) : pool_(pool) {}
    bool await_ready() const noexcept { return !pool_.conns.empty(); }
    bool await_suspend(std::coroutine_handle<> h) const noexcept {
      if (pool_.conns.empty()) {
        pool_.waiters.push(h);
        return true;
      }
      return false;
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
size_t nr_peers = 0;
size_t me = 0;

size_t get_shard(std::string_view key) {
  if (nr_peers == 0)
    return 0;
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
      if (n < 0) {
        fmt::print("writev_all error: {}\n", strerror(-n));
      }
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

constexpr size_t kRPCReplyHeaderSize = sizeof(uint64_t) + sizeof(uint32_t);
uringpp::task<void> rpc_reply_header(uringpp::socket &conn, uint64_t req_id,
                                     uint32_t body_len) {
  char header[kRPCReplyHeaderSize];
  auto p = header;
  *(uint64_t *)p = req_id;
  p += sizeof(uint64_t);
  *(uint32_t *)p = body_len;
  p += sizeof(uint32_t);
  co_await send_all(conn, header, kRPCReplyHeaderSize);
}

uringpp::task<void> rpc_reply(uringpp::socket &conn, uint64_t req_id,
                              std::vector<char> data) {
  char header[kRPCReplyHeaderSize];
  auto len = static_cast<uint32_t>(data.size());
  auto p = header;
  *(uint64_t *)p = req_id;
  p += sizeof(uint64_t);
  *(uint32_t *)p = len;
  p += sizeof(uint32_t);
  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = kRPCReplyHeaderSize;
  iov[1].iov_base = const_cast<char *>(data.data());
  iov[1].iov_len = len;
  co_await writev_all(conn, iov, data.empty() ? 1 : 2);
}

constexpr size_t kRPCRequestHeaderSize =
    sizeof(uint64_t) + sizeof(method) + sizeof(uint32_t);

task<void> recv_n(uringpp::socket &conn, io_buffer &buf, size_t n) {
  if (buf.writable() < n) {
    buf.expand(n * 2);
  }
  while (buf.readable() < n) {
    int recved = co_await conn.recv(buf.write_data(), buf.writable());
    if (recved <= 0) {
      throw std::runtime_error("connection closed");
    }
    buf.advance_write(recved);
  }
}

task<std::vector<key_value>>
stream_recv_list_rpc(uringpp::socket &conn, size_t req_size, io_buffer &buf) {
  if (req_size == 0) {
    co_return {};
  }
  std::vector<key_value> result;
  co_await recv_n(conn, buf, sizeof(uint32_t));
  auto count = *(uint32_t *)buf.read_data();
  buf.advance_read(sizeof(uint32_t));

  for (size_t i = 0; i < count; i++) {
    co_await recv_n(conn, buf, sizeof(uint32_t));
    auto len = *(uint32_t *)buf.read_data();
    buf.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, buf, len);
    auto key = std::string(buf.read_data(), len);
    buf.advance_read(len);
    auto value = store->query(key);
    if (value.first) {
      result.push_back({key, std::string(value.first, value.second)});
    }
  }
  co_return result;
}

task<void> stream_recv_batch_rpc(uringpp::socket &conn, size_t req_size,
                                 io_buffer &buf) {
  if (req_size == 0) {
    co_return;
  }
  co_await recv_n(conn, buf, sizeof(uint32_t));
  auto count = *(uint32_t *)buf.read_data();
  buf.advance_read(sizeof(uint32_t));

  for (size_t i = 0; i < count; i++) {
    co_await recv_n(conn, buf, sizeof(uint32_t));
    auto len = *(uint32_t *)buf.read_data();
    buf.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, buf, len);
    auto key = std::string(buf.read_data(), len);
    buf.advance_read(len);

    co_await recv_n(conn, buf, sizeof(uint32_t));
    len = *(uint32_t *)buf.read_data();
    buf.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, buf, len);
    auto value = std::string(buf.read_data(), len);
    buf.advance_read(len);

    store->add(key, value);
  }
}

task<void> handle_rpc(uringpp::socket conn, size_t conn_id) {
  auto loop = rpc_loops[conn_id % rpc_loops.size()];
  co_await loop->switch_to_io_thread();
  io_buffer request(65536);
  int state = kStateRecvHeader;
  uint64_t req_id = 0;
  uint32_t req_size = 0;
  method m = method::query;
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
        request.advance_read(kRPCRequestHeaderSize);
        state = kStateRecvBody;
        if (req_size > request.writable()) {
          request.expand(req_size - request.writable());
        }
        if (m == method::batch) {
          co_await stream_recv_batch_rpc(conn, req_size, request);
          co_await rpc_reply(conn, req_id, {});
          state = kStateRecvHeader;
          continue;
        } else if (m == method::list) {
          auto values = co_await stream_recv_list_rpc(conn, req_size, request);
          size_t rep_body_size = sizeof(uint32_t);
          for (auto &v : values) {
            rep_body_size +=
                sizeof(uint32_t) * 2 + v.key.size() + v.value.size();
          }
          co_await rpc_reply_header(conn, req_id, rep_body_size);
          auto rep_body = std::vector<char>(rep_body_size);
          auto p = &rep_body[0];
          auto last_write_cursor = p;
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
            if (p - last_write_cursor > 32 * 1024) {
              co_await send_all(conn, last_write_cursor, p - last_write_cursor);
              last_write_cursor = p;
            }
          }
          if (p - last_write_cursor > 0) {
            co_await send_all(conn, last_write_cursor, p - last_write_cursor);
          }
          state = kStateRecvHeader;
          continue;
        }
      } else if (request.writable() < kRPCRequestHeaderSize) {
        request.expand(kRPCRequestHeaderSize - request.writable());
      }
    }
    if (state == kStateRecvBody) {
      if (request.readable() >= req_size) {
        state = kStateProcess;
      }
    }
    if (state != kStateProcess) {
      auto n = co_await conn.recv(request.write_data(), request.writable());
      if (n <= 0) {
        fmt::print("recv error: {} writable {}\n", n, request.writable());
        co_return;
      }
      request.advance_write(n);
      continue;
    }

    auto req_p = request.read_data();
    switch (m) {
    case method::query: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size;
      auto value = store->query(key);
      if (value.first) {
        auto size_u32 = static_cast<uint32_t>(value.second);
        auto rep_body_size = value.second + sizeof(size_u32);
        auto rep_body = std::vector<char>(rep_body_size);
        std::copy_n(reinterpret_cast<char *>(&size_u32), sizeof(size_u32),
                    rep_body.begin());
        std::copy_n(value.first, value.second,
                    rep_body.begin() + sizeof(size_u32));
        co_await rpc_reply(conn, req_id, std::move(rep_body));
      } else {
        co_await rpc_reply(conn, req_id, {});
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
      co_await rpc_reply(conn, req_id, {});
      store->add(key, value);
    } break;
    case method::del: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size;
      co_await rpc_reply(conn, req_id, {});
      store->del(key);
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
      co_await rpc_reply(conn, req_id, {});
      store->zadd(key, value, score);
    } break;
    case method::zrmv: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size;
      uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(value_size);
      std::string_view value(req_p, value_size);
      req_p += value_size;
      co_await rpc_reply(conn, req_id, {});
      store->zrmv(key, value);
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
        co_await rpc_reply_header(conn, req_id, rep_body_size);
        auto rep_body = std::vector<char>(rep_body_size);
        auto p = &rep_body[0];
        auto last_write_cursor = p;
        *reinterpret_cast<uint32_t *>(p) = score_values.size();
        p += sizeof(uint32_t);
        for (auto &sv : score_values) {
          *reinterpret_cast<uint32_t *>(p) = sv.value.size();
          p += sizeof(uint32_t);
          std::copy_n(sv.value.data(), sv.value.size(), p);
          p += sv.value.size();
          *reinterpret_cast<uint32_t *>(p) = sv.score;
          p += sizeof(uint32_t);
          if (p - last_write_cursor > 32 * 1024) {
            co_await send_all(conn, last_write_cursor, p - last_write_cursor);
            last_write_cursor = p;
          }
        }
        if (p - last_write_cursor > 0) {
          co_await send_all(conn, last_write_cursor, p - last_write_cursor);
        }
      }
    } break;
    default: {
      co_await rpc_reply(conn, req_id, {});
    } break;
    }
    request.advance_read(req_size);
    state = kStateRecvHeader;
    req_size = 0;
  }
}

struct pending_call {
  uringpp::socket *conn;
  void *callback;
  std::coroutine_handle<> h;
  std::vector<char> response;
  method m = method::query;
};

task<std::vector<char>> send_rpc_request(conn_pool &pool,
                                         std::vector<char> body, size_t len,
                                         pending_call *pending) {
  struct iovec iov[1];
  iov[0].iov_base = (void *)&body[0];
  iov[0].iov_len = len;
  auto conn = co_await pool.get_conn();
  co_await writev_all(*conn->conn, iov, 1);
  pool.put_conn(conn);
  co_await std::suspend_always{};
  co_return std::move(pending->response);
}

task<std::vector<char>> rpc_call(conn_pool &pool, std::vector<char> body,
                                 size_t len) {
  auto pending = new pending_call;
  *reinterpret_cast<uint64_t *>(&body[0]) = reinterpret_cast<uint64_t>(pending);
  auto t = send_rpc_request(pool, std::move(body), len, pending);
  pending->h = t.h_;
  return t;
}

task<std::optional<std::pair<std::vector<char>, std::string_view>>>
remote_query(conn_pool &pool, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::query;
  *reinterpret_cast<uint32_t *>(
      &header_body[sizeof(uint64_t) + sizeof(method)]) = body_size;
  *reinterpret_cast<uint32_t *>(
      &header_body[sizeof(uint64_t) + sizeof(method) + sizeof(uint32_t)]) =
      key.size();
  std::copy_n(key.data(), key.size(),
              header_body.begin() + kRPCRequestHeaderSize + sizeof(uint32_t));
  auto response = co_await rpc_call(pool, std::move(header_body),
                                    kRPCRequestHeaderSize + body_size);
  if (response.empty()) {
    co_return std::nullopt;
  }
  auto p = response.data();
  auto value_size = *reinterpret_cast<uint32_t *>(p);
  p += sizeof(value_size);
  co_return std::make_pair(std::move(response),
                           std::string_view(p, value_size));
}

task<void> remote_add(conn_pool &pool, std::string_view key,
                      std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::add;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<void> remote_del(conn_pool &pool, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::del;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<void> remote_batch_send_body(conn_pool &pool,
                                  std::vector<key_value_view> const &kvs,
                                  std::vector<char> &header_body) {
  auto conn = co_await pool.get_conn();
  co_await send_all(*conn->conn, header_body.data(), kRPCRequestHeaderSize);
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  auto last_write_cursor = body_p;
  *reinterpret_cast<uint32_t *>(body_p) = kvs.size();
  body_p += sizeof(uint32_t);
  for (auto const kv : kvs) {
    *reinterpret_cast<uint32_t *>(body_p) = kv.key.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.key.data(), kv.key.size(), body_p);
    body_p += kv.key.size();
    *reinterpret_cast<uint32_t *>(body_p) = kv.value.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.value.data(), kv.value.size(), body_p);
    body_p += kv.value.size();
    if (body_p - last_write_cursor > 256 * 1024) {
      co_await send_all(*conn->conn, last_write_cursor,
                        body_p - last_write_cursor);
      last_write_cursor = body_p;
    }
  }
  if (body_p - last_write_cursor > 0) {
    co_await send_all(*conn->conn, last_write_cursor,
                      body_p - last_write_cursor);
  }
  pool.put_conn(conn);
  co_await std::suspend_always{};
}

task<void> remote_batch(conn_pool &pool,
                        std::vector<key_value_view> const &kvs) {
  auto body_size = sizeof(uint32_t);
  for (auto const kv : kvs) {
    body_size += sizeof(uint32_t) * 2 + kv.key.size() + kv.value.size();
  }
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  auto pending = new pending_call;
  *reinterpret_cast<uint64_t *>(&header_body[0]) =
      reinterpret_cast<uint64_t>(pending);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::batch;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto t = remote_batch_send_body(pool, kvs, header_body);
  pending->h = t.h_;
  co_await t;
}

task<void>
remote_list_send_body(conn_pool &pool,
                      std::unordered_set<std::string_view> const &keys,
                      std::vector<char> &header_body) {
  auto conn = co_await pool.get_conn();
  co_await send_all(*conn->conn, header_body.data(), kRPCRequestHeaderSize);
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  auto last_write_cursor = body_p;
  *reinterpret_cast<uint32_t *>(body_p) = keys.size();
  body_p += sizeof(uint32_t);
  for (auto const key : keys) {
    *reinterpret_cast<uint32_t *>(body_p) = key.size();
    body_p += sizeof(uint32_t);
    std::copy_n(key.data(), key.size(), body_p);
    body_p += key.size();
    if (body_p - last_write_cursor > 256 * 1024) {
      co_await send_all(*conn->conn, last_write_cursor,
                        body_p - last_write_cursor);
      last_write_cursor = body_p;
    }
  }
  if (body_p - last_write_cursor > 0) {
    co_await send_all(*conn->conn, last_write_cursor,
                      body_p - last_write_cursor);
  }
  pool.put_conn(conn);
  co_await std::suspend_always{};
}

task<void>
remote_list(conn_pool &pool, std::unordered_set<std::string_view> const &keys,
            std::function<task<void>(size_t, size_t, std::string_view,
                                     std::string_view)> *callback) {
  auto body_size = sizeof(uint32_t);
  for (auto const &key : keys) {
    body_size += sizeof(uint32_t) + key.size();
  }
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  auto pending = new pending_call;
  *reinterpret_cast<uint64_t *>(&header_body[0]) =
      reinterpret_cast<uint64_t>(pending);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::list;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto t = remote_list_send_body(pool, keys, header_body);
  pending->m = method::list;
  pending->h = t.h_;
  pending->callback = callback;
  co_await t;
}

task<void> remote_zadd(conn_pool &pool, std::string_view key,
                       std::string_view value, uint32_t score) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 3;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zadd;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  body_p += value.size();
  *reinterpret_cast<uint32_t *>(body_p) = score;
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<void>
list_stream_callback(uringpp::socket &conn, size_t rep_size,
                     io_buffer &response,
                     std::function<task<void>(size_t, size_t, std::string_view,
                                              std::string_view)> *callback) {
  if (rep_size == 0) {
    co_return;
  }
  co_await recv_n(conn, response, sizeof(uint32_t));
  auto count = *reinterpret_cast<uint32_t *>(response.read_data());
  response.advance_read(sizeof(uint32_t));
  size_t processed = 0;
  while (processed < count) {
    co_await recv_n(conn, response, sizeof(uint32_t));
    auto key_size = *reinterpret_cast<uint32_t *>(response.read_data());
    response.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, response, key_size);
    auto key = std::string(response.read_data(), key_size);
    response.advance_read(key_size);

    co_await recv_n(conn, response, sizeof(uint32_t));
    auto value_size = *reinterpret_cast<uint32_t *>(response.read_data());
    response.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, response, value_size);
    auto value = std::string(response.read_data(), value_size);
    response.advance_read(value_size);

    co_await (*callback)(processed, count, key, value);
    processed++;
  }
  co_await (*callback)(count, count, std::string_view(), std::string_view());
}

task<void> zrange_stream_callback(
    uringpp::socket &conn, size_t rep_size, io_buffer &response,
    std::function<task<void>(size_t, size_t, std::string_view, uint32_t)>
        *callback) {
  if (rep_size == 0) {
    co_return;
  }
  co_await recv_n(conn, response, sizeof(uint32_t));
  auto count = *reinterpret_cast<uint32_t *>(response.read_data());
  response.advance_read(sizeof(uint32_t));
  size_t processed = 0;
  while (processed < count) {
    co_await recv_n(conn, response, sizeof(uint32_t));
    auto value_size = *reinterpret_cast<uint32_t *>(response.read_data());
    response.advance_read(sizeof(uint32_t));

    co_await recv_n(conn, response, value_size);
    auto value = std::string(response.read_data(), value_size);
    response.advance_read(value_size);

    co_await recv_n(conn, response, sizeof(uint32_t));
    auto score = *reinterpret_cast<uint32_t *>(response.read_data());
    response.advance_read(sizeof(uint32_t));

    co_await (*callback)(processed, count, value, score);
    processed += 1;
  }
  co_await (*callback)(count, count, std::string_view(), 0);
}

task<void> remote_zrange(
    conn_pool &pool, std::string_view key, uint32_t min_score,
    uint32_t max_score,
    std::function<task<void>(size_t, size_t, std::string_view, uint32_t)>
        *callback) {
  auto body_size = sizeof(uint32_t) * 3 + key.size();
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  auto pending = new pending_call;
  pending->m = method::zrange;
  *reinterpret_cast<uint64_t *>(&header_body[0]) =
      reinterpret_cast<uint64_t>(pending);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zrange;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = min_score;
  body_p += sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(body_p) = max_score;
  auto conn = co_await pool.get_conn();
  auto fn = [&]() -> task<void> {
    co_await send_all(*conn->conn, header_body.data(), header_body.size());
    pool.put_conn(conn);
    co_await std::suspend_always{};
  };
  auto t = fn();
  pending->h = t.h_;
  pending->callback = callback;
  co_await t;
}

task<void> remote_zrmv(conn_pool &pool, std::string_view key,
                       std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zrmv;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<void> rpc_reply_recv_loop(uringpp::socket &rpc_conn) {
  io_buffer response(65536);
  int state = kStateRecvHeader;
  size_t rep_size = 0;
  uint64_t req_id = 0;
  while (true) {
    if (state == kStateRecvHeader) {
      if (response.readable() >= kRPCReplyHeaderSize) {
        auto p = response.read_data();
        req_id = *reinterpret_cast<uint64_t *>(p);
        p += sizeof(req_id);
        rep_size = *reinterpret_cast<uint32_t *>(p);
        p += sizeof(rep_size);
        state = kStateRecvBody;
        response.advance_read(kRPCReplyHeaderSize);
        auto pending = reinterpret_cast<pending_call *>(req_id);
        if (response.writable() < rep_size) {
          response.expand(rep_size - response.writable());
        }
        if (pending->m == method::zrange || pending->m == method::list) {
          if (pending->m == method::zrange) {
            auto callback = reinterpret_cast<std::function<task<void>(
                size_t, size_t, std::string_view, uint32_t)> *>(
                pending->callback);
            co_await zrange_stream_callback(rpc_conn, rep_size, response,
                                            callback);
          } else {
            auto callback = reinterpret_cast<std::function<task<void>(
                size_t, size_t, std::string_view, std::string_view)> *>(
                pending->callback);
            co_await list_stream_callback(rpc_conn, rep_size, response,
                                          callback);
          }
          pending->h.resume();
          delete pending;
          state = kStateRecvHeader;
          rep_size = 0;
          continue;
        }
      } else if (response.writable() < kRPCReplyHeaderSize) {
        response.expand(kRPCReplyHeaderSize - response.writable());
      }
    }
    if (state == kStateRecvBody) {
      if (response.readable() >= rep_size) {
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
    pending->response.assign(response.read_data(),
                             response.read_data() + rep_size);
    response.advance_read(rep_size);
    pending->h.resume();
    delete pending;
    state = kStateRecvHeader;
    rep_size = 0;
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
    std::list<output_page> send_buf;
    std::list<output_page> pending_send_buf;
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
        request.expand(simdjson::SIMDJSON_PADDING - request.writable());
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
        if (n <= 0) [[unlikely]] {
          if (n < 0) {
            throw std::runtime_error(
                fmt::format("recv error: {}\n", strerror(-n)));
          }
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
            if (!value.first) {
              co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              co_await send_text(conn, value.first, value.second);
              delete[] value.first;
            }
          } else {
            auto remote_value =
                co_await remote_query(rpc_clients[key_shard][conn_shard], key);
            if (!remote_value.has_value()) {
              co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              co_await send_text(
                  conn, const_cast<char *>(remote_value->second.data()),
                  remote_value->second.size());
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
          if (key_shard == me) {
            store->zrmv(key, value);
          } else {
            co_await remote_zrmv(rpc_clients[key_shard][conn_shard], key,
                                 value);
          }
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
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
          simdjson::padded_string_view padded(body_start, content_length,
                                              request.capacity() -
                                                  request.read_idx());
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
          simdjson::padded_string_view json(body_start, content_length,
                                            request.capacity() -
                                                request.read_idx());
          auto doc = parser.parse(json);
          auto const key = doc["key"].get_string().take_value();
          auto const value = doc["value"].get_string().take_value();
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            store->add(key, value);
          } else {
            co_await remote_add(rpc_clients[key_shard][conn_shard], key, value);
          }
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'b': {
          // batch
          simdjson::padded_string_view json(body_start, content_length,
                                            request.capacity() -
                                                request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::vector<std::vector<key_value_view>> sharded_keys(nr_peers);
          for (auto &&kv : arr) {
            auto const key = kv["key"].get_string().take_value();
            auto const value = kv["value"].get_string().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              store->add(key, value);
            } else {
              sharded_keys[key_shard].emplace_back(key, value);
            }
          }
          std::vector<task<void>> tasks;
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            tasks.emplace_back(remote_batch(rpc_clients[key_shard][conn_shard],
                                            sharded_keys[key_shard]));
          }
          for (auto &&task : tasks) {
            co_await task;
          }
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'l': {
          // list
          simdjson::padded_string_view json(body_start, content_length,
                                            request.capacity() -
                                                request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::unordered_set<std::string_view> keys;
          std::vector<std::unordered_set<std::string_view>> sharded_keys(
              nr_peers);
          for (auto &&k : arr) {
            auto key = k.get_string().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              keys.insert(key);
            } else {
              sharded_keys[key_shard].insert(key);
            }
          }
          bool is_first_kv_pair = true;
          auto remote_kv_count = 0;
          std::vector<task<void>> tasks;
          std::vector<std::function<task<void>(size_t, size_t, std::string_view,
                                               std::string_view)> *>
              callbacks;
          auto write_kv = [](rapidjson::StringBuffer &buffer,
                             std::string_view key, std::string_view value) {
            auto d = rapidjson::Document();
            d.SetObject();
            auto &a = d.GetAllocator();
            d.AddMember("key", rapidjson::StringRef(key.data(), key.size()), a);
            d.AddMember("value",
                        rapidjson::StringRef(value.data(), value.size()), a);
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            d.Accept(writer);
          };
          constexpr size_t batch_size = 50;
          bool first = true;
          size_t batch_idx = 0;
          size_t chunk_size = 0;
          rapidjson::StringBuffer buffer[batch_size];
          char chunk_length[32];
          const char chunk_end[] = "\r\n";
          struct iovec iov[batch_size + 2];
          iov[0].iov_base = chunk_length;
          auto send_kv_chunk = [&]() -> task<void> {
            for (size_t i = 0; i < batch_idx; ++i) {
              iov[i + 1].iov_base = (void *)buffer[i].GetString();
              iov[i + 1].iov_len = buffer[i].GetSize();
              chunk_size += buffer[i].GetSize();
            }
            iov[batch_idx + 1].iov_base = (void *)chunk_end;
            iov[batch_idx + 1].iov_len = 2;
            auto chunk_size_len = itohex(chunk_length, chunk_size);
            chunk_length[chunk_size_len++] = '\r';
            chunk_length[chunk_size_len++] = '\n';
            iov[0].iov_len = chunk_size_len;
            co_await writev_all(conn, iov, batch_idx + 2);
            for (size_t i = 0; i < batch_idx; ++i) {
              buffer[i].Clear();
            }
            batch_idx = 0;
            chunk_size = 0;
          };
          auto kv_callback = [&](std::string_view key,
                                 std::string_view value) -> task<void> {
            if (is_first_kv_pair) {
              co_await send_all(conn, CHUNK_RESPONSE,
                                sizeof(CHUNK_RESPONSE) - 1);
              is_first_kv_pair = false;
            } else {
              buffer[batch_idx].Put(',');
            }
            write_kv(buffer[batch_idx], key, value);
            if (++batch_idx == batch_size) {
              co_await send_kv_chunk();
            }
          };
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            callbacks.push_back(new std::function(
                [&](size_t current, size_t total, std::string_view key,
                    std::string_view value) -> task<void> {
                  if (current != total) {
                    co_await kv_callback(key, value);
                  }
                }));
            tasks.emplace_back(remote_list(rpc_clients[key_shard][conn_shard],
                                           sharded_keys[key_shard],
                                           callbacks.back()));
          }
          auto local_key_values = store->list(keys.begin(), keys.end());
          for (auto const &kv : local_key_values) {
            co_await kv_callback(kv.key, kv.value);
          }
          for (auto &&t : tasks) {
            co_await t;
          }
          for (auto cb : callbacks) {
            delete cb;
          }
          if (is_first_kv_pair) {
            co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            if (batch_idx > 0) {
              co_await send_kv_chunk();
            }
            co_await send_all(conn, CHUNK_END, sizeof(CHUNK_END) - 1);
          }
        } break;
        case 'z': {
          switch (path[2]) {
          case 'a': {
            // zadd
            std::string_view key(&path[6], path_len - 6);
            simdjson::padded_string_view json(body_start, content_length,
                                              request.capacity() -
                                                  request.read_idx());
            auto score_value = parser.parse(json);
            auto value = score_value["value"].get_string().take_value();
            auto score = score_value["score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              store->zadd(key, value, score);
            } else {
              co_await remote_zadd(rpc_clients[key_shard][conn_shard], key,
                                   value, score);
            }
            co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          } break;
          case 'r': {
            // zrange
            std::string_view key(&path[8], path_len - 8);
            simdjson::padded_string_view json(body_start, content_length,
                                              request.capacity() -
                                                  request.read_idx());
            auto score_range = parser.parse(json);
            auto min_score = score_range["min_score"].get_uint64().take_value();
            auto max_score = score_range["max_score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            auto write_sv = [&](rapidjson::StringBuffer &buffer,
                                std::string_view value, uint32_t score) {
              auto d = rapidjson::Document();
              auto &sv_object = d.SetObject();
              auto &allocator = d.GetAllocator();
              auto v_string = rapidjson::StringRef(value.data(), value.size());
              sv_object.AddMember("value", v_string, allocator);
              sv_object.AddMember("score", score, allocator);
              rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
              d.Accept(writer);
            };
            constexpr size_t batch_size = 50;
            bool first = true;
            char left_bracket = '[';
            char comma = ',';
            size_t batch_idx = 0;
            size_t chunk_size = 0;
            rapidjson::StringBuffer buffer[batch_size];
            char chunk_length[32];
            const char chunk_end[] = "\r\n";
            struct iovec iov[batch_size + 2];
            iov[0].iov_base = chunk_length;
            auto send_batch_chunk = [&]() -> task<void> {
              for (size_t i = 0; i < batch_idx; ++i) {
                iov[i + 1].iov_base = (void *)buffer[i].GetString();
                iov[i + 1].iov_len = buffer[i].GetSize();
                chunk_size += buffer[i].GetSize();
              }
              iov[batch_idx + 1].iov_base = (void *)chunk_end;
              iov[batch_idx + 1].iov_len = 2;
              auto chunk_size_len = itohex(chunk_length, chunk_size);
              chunk_length[chunk_size_len++] = '\r';
              chunk_length[chunk_size_len++] = '\n';
              iov[0].iov_len = chunk_size_len;
              co_await writev_all(conn, iov, batch_idx + 2);
              for (size_t i = 0; i < batch_idx; ++i) {
                buffer[i].Clear();
              }
              batch_idx = 0;
              chunk_size = 0;
            };
            if (key_shard == me) {
              auto score_values = store->zrange(key, min_score, max_score);
              if (!score_values.has_value()) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
              } else if (score_values->empty()) {
                co_await send_all(conn, EMPTY_ARRAY, sizeof(EMPTY_ARRAY) - 1);
              } else {
                co_await send_all(conn, CHUNK_RESPONSE,
                                  sizeof(CHUNK_RESPONSE) - 1);
                uint32_t sv_written = 0;
                for (auto const sv : score_values.value()) {
                  auto &buf = buffer[batch_idx];
                  write_sv(buffer[batch_idx], sv.value, sv.score);
                  if (++sv_written != score_values->size()) {
                    buf.Put(',');
                  }
                  if (++batch_idx == batch_size) {
                    co_await send_batch_chunk();
                  }
                }
                if (batch_idx > 0) {
                  co_await send_batch_chunk();
                }
                co_await send_all(conn, CHUNK_END, sizeof(CHUNK_END) - 1);
              }
            } else {
              bool has_value = false;
              auto callback = new std::function<task<void>(
                  size_t, size_t, std::string_view, uint32_t)>(
                  [&](size_t current, size_t total, std::string_view value,
                      uint32_t score) -> task<void> {
                    if (!has_value) {
                      has_value = true;
                      co_await send_all(conn, CHUNK_RESPONSE,
                                        sizeof(CHUNK_RESPONSE) - 1);
                    }
                    if (current != total) {
                      auto &buf = buffer[batch_idx];
                      write_sv(buffer[batch_idx], value, score);
                      if (current + 1 != total) {
                        buf.Put(',');
                      }
                      if (++batch_idx == batch_size) {
                        co_await send_batch_chunk();
                      }
                    } else {
                      if (batch_idx > 0) {
                        co_await send_batch_chunk();
                      }
                      co_await send_all(conn, CHUNK_END, sizeof(CHUNK_END) - 1);
                    }
                  });
              co_await remote_zrange(rpc_clients[key_shard][conn_shard], key,
                                     min_score, max_score, callback);
              delete callback;
              if (!has_value) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
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
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", "8080");
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

int main(int argc, char *argv[]) {
  main_loop = loop_with_queue::create(32768);
  main_loop->waker().detach();
  ::signal(SIGPIPE, SIG_IGN);
  cores = get_cpu_affinity();
  loops.resize(cores.size());
  rpc_loops.resize(cores.size());
  cds::Initialize();
  cds::gc::HP hpGC;
  store = std::make_unique<storage>();
  {
    auto kv_thread = std::thread([]() {
      store->load_kv();
    });
    auto zset_thread = std::thread([]() {
      store->load_zset();
    });
    kv_thread.join();
    zset_thread.join();
  }
  auto flush_thread = std::thread([]() {
    for (;;) {
      store->flush();
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  });
  flush_thread.detach();
  bind_cpu(cores[0]);
  for (int i = 0; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i]] {
      fmt::print("thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      auto loop = loop_with_queue::create(32768, IORING_SETUP_ATTACH_WQ,
                                          main_loop->fd());
      loops[i] = loop;
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
  loop_started = 0;
  for (int i = 0; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i]] {
      fmt::print("RPC Thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      auto loop = loop_with_queue::create(32768, IORING_SETUP_ATTACH_WQ,
                                          main_loop->fd());
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
  for (;;) {
    main_loop->poll();
  }
  cds::Terminate();
  return 0;
}
