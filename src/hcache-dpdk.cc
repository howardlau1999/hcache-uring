#include "f-stack-co/event_loop.h"
#include "f-stack-co/init.h"
#include "f-stack-co/task.h"
#include "f-stack-co/tcp_connection.h"
#include "f-stack-co/tcp_listener.h"
#include "fmt/format.h"
#include "io_buffer.h"
#include "picohttpparser/picohttpparser.h"
#include "storage.h"
#include "threading.h"
#include <fstream>
#include <memory>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <simdjson.h>

using fstackco::task;

struct send_conn_state {
  std::unique_ptr<fstackco::tcp_connection> conn;
  std::list<output_page> send_buf;
  std::list<output_page> pending_send_buf;
  bool sending;
  bool closed;
};

struct conn_pool {
  size_t next = 0;
  std::vector<send_conn_state> conns;
  send_conn_state &get_conn() {
    auto &conn = conns[next];
    next = (next + 1) % conns.size();
    return conn;
  }
};

std::vector<int> event_fds;
std::vector<int> rpc_event_fds;
std::vector<io_queue> io_queues;
std::vector<io_queue> rpc_io_queues;
std::vector<std::shared_ptr<fstackco::event_loop>> loops;
std::vector<std::shared_ptr<fstackco::event_loop>> rpc_loops;
std::vector<std::vector<conn_pool>> rpc_clients;
std::atomic<size_t> loop_started = 0;
std::atomic<size_t> clients_connected = 0;
std::unique_ptr<storage> store;
io_queue main_io_queue;
std::shared_ptr<fstackco::event_loop> main_loop;
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

const char HTTP_404[] = "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n";
const char HTTP_400[] = "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n";
const char OK_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
const char EMPTY_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
const char LOADING_RESPONSE[] =
    "HTTP/1.1 200 OK\r\ncontent-length: 7\r\n\r\nloading";

auto format_200_header(size_t body_length) {
  return fmt::format("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
                     body_length);
}

task<void> send_worker(fstackco::tcp_connection &conn,
                       std::list<output_page> &buffer,
                       std::list<output_page> &pending, bool &sending,
                       bool &closed) {
  sending = true;
  while (buffer.size() > 0) {
    for (auto &page : buffer) {
      while (page.sent < page.size) {
        auto sent =
            co_await conn.write(page.data + page.sent, page.size - page.sent);
        if (sent == 0) {
          closed = true;
          co_return;
        }
        page.sent += sent;
      }
    }
    buffer.clear();
    if (!pending.empty()) {
      std::swap(buffer, pending);
    }
  }
  sending = false;
}

void send_all(fstackco::tcp_connection &conn, bool &sending, bool &closed,
              std::list<output_page> &send_buf,
              std::list<output_page> &pending_send_buf, char const *data,
              size_t len, bool owned = false) {
  if (closed)
    return;
  if (sending) {
    pending_send_buf.emplace_back(const_cast<char *>(data), len, 0, owned);
  } else {
    send_buf.emplace_back(const_cast<char *>(data), len, 0, owned);
    send_worker(conn, send_buf, pending_send_buf, sending, closed).detach();
  }
}

void send_all(fstackco::tcp_connection &conn, bool &sending, bool &closed,
              std::list<output_page> &send_buf,
              std::list<output_page> &pending_send_buf,
              std::list<output_page> &pages) {
  if (closed)
    return;
  if (sending) {
    pending_send_buf.splice(pending_send_buf.end(), pages);
  } else {
    send_buf.splice(send_buf.end(), pages);
    send_worker(conn, send_buf, pending_send_buf, sending, closed).detach();
  }
}

void send_json(fstackco::tcp_connection &conn, bool &sending, bool &closed,
               std::list<output_page> &send_buf,
               std::list<output_page> &pending_send_buf,
               json_output_stream &buffer) {
  {
    auto header = format_200_header(buffer.GetSize());
    auto data = new char[header.size()];
    std::copy_n(header.data(), header.size(), data);
    send_all(conn, sending, closed, send_buf, pending_send_buf, data,
             header.size(), true);
  }
  send_all(conn, sending, closed, send_buf, pending_send_buf, buffer.pages_);
}

void send_text(fstackco::tcp_connection &conn, bool &sending, bool &closed,
               std::list<output_page> &send_buf,
               std::list<output_page> &pending_send_buf, char *data,
               size_t len) {
  {
    auto header = format_200_header(len);
    auto header_data = new char[header.size()];
    std::copy_n(header.data(), header.size(), header_data);
    send_all(conn, sending, closed, send_buf, pending_send_buf, header_data,
             header.size(), true);
  }
  send_all(conn, sending, closed, send_buf, pending_send_buf, data, len, true);
}

constexpr int kStateRecvHeader = 0;
constexpr int kStateRecvBody = 1;
constexpr int kStateProcess = 2;

constexpr size_t kRPCReplyHeaderSize = sizeof(uint64_t) + sizeof(uint32_t);
char *rpc_reply_header(uint64_t req_id, uint32_t len) {
  auto header = new char[kRPCReplyHeaderSize];
  std::copy_n(reinterpret_cast<char *>(&req_id), sizeof(req_id), header);
  std::copy_n(reinterpret_cast<char *>(&len), sizeof(len),
              header + sizeof(req_id));
  return header;
}

constexpr size_t kRPCRequestHeaderSize =
    sizeof(uint64_t) + sizeof(method) + sizeof(uint32_t);

task<void> handle_rpc(fstackco::tcp_connection conn, size_t conn_id) {
  auto loop = rpc_loops[conn_id % rpc_loops.size()];
  co_await rpc_io_queues[conn_id % rpc_io_queues.size()].queue_in_loop(
      rpc_event_fds[0]);
  io_buffer request(65536);
  int state = kStateRecvHeader;
  uint64_t req_id = 0;
  uint32_t req_size = 0;
  method m = method::query;
  bool sending = false;
  bool closed = false;
  std::list<output_page> send_buf;
  std::list<output_page> pending_send_buf;
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
        request.advance_read(p - request.read_data());
        state = kStateRecvBody;
        if (req_size > request.writable()) {
          request.expand(req_size - request.writable());
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
      auto n = co_await conn.read(request.write_data(), request.writable());
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
        auto header = rpc_reply_header(req_id, value.second + sizeof(uint32_t));
        auto size_u32 = static_cast<uint32_t>(value.second);
        auto rep_body = new char[value.second + sizeof(size_u32)];
        std::copy_n(reinterpret_cast<char *>(&size_u32), sizeof(size_u32),
                    rep_body);
        std::copy_n(value.first, value.second, rep_body + sizeof(size_u32));
        send_all(conn, sending, closed, send_buf, pending_send_buf, header,
                 kRPCReplyHeaderSize, true);
        send_all(conn, sending, closed, send_buf, pending_send_buf, rep_body,
                 value.second + sizeof(size_u32), true);
      } else {
        auto header = rpc_reply_header(req_id, 0);
        send_all(conn, sending, closed, send_buf, pending_send_buf, header,
                 kRPCReplyHeaderSize, true);
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
      store->add(key, value);
      auto header = rpc_reply_header(req_id, 0);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
    } break;
    case method::del: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size;
      store->del(key);
      auto header = rpc_reply_header(req_id, 0);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
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
      auto rep_body = new char[rep_body_size];
      auto p = rep_body;
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
      auto header = rpc_reply_header(req_id, rep_body_size);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
      send_all(conn, sending, closed, send_buf, pending_send_buf, rep_body,
               rep_body_size, true);
    } break;
    case method::batch: {
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
      auto header = rpc_reply_header(req_id, 0);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
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
      auto header = rpc_reply_header(req_id, 0);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
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
      store->zrmv(key, value);
      auto header = rpc_reply_header(req_id, 0);
      send_all(conn, sending, closed, send_buf, pending_send_buf, header,
               kRPCReplyHeaderSize, true);
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
        auto header = rpc_reply_header(req_id, 0);
        send_all(conn, sending, closed, send_buf, pending_send_buf, header,
                 kRPCReplyHeaderSize, true);
      } else {
        auto const &score_values = values.value();
        size_t rep_body_size = sizeof(uint32_t);
        for (auto &v : score_values) {
          rep_body_size += sizeof(uint32_t) * 2 + v.value.size();
        }
        auto rep_body = new char[rep_body_size];
        auto p = rep_body;
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
        auto header = rpc_reply_header(req_id, rep_body_size);
        send_all(conn, sending, closed, send_buf, pending_send_buf, header,
                 kRPCReplyHeaderSize, true);
        send_all(conn, sending, closed, send_buf, pending_send_buf, rep_body,
                 rep_body_size, true);
      }
    } break;
    }
    request.advance_read(req_size);
    state = kStateRecvHeader;
    req_size = 0;
  }
}

struct pending_call {
  std::coroutine_handle<> h;
  std::vector<char> response;
};

task<std::vector<char>>
send_rpc_request(fstackco::tcp_connection &rpc_conn,
                 std::list<output_page> &send_buf,
                 std::list<output_page> &pending_send_buf, bool &sending,
                 bool &closed, char *body, size_t len, pending_call *pending) {
  co_await std::suspend_always{};
  send_all(rpc_conn, sending, closed, send_buf, pending_send_buf, body, len,
           true);
  co_await std::suspend_always{};
  co_return std::move(pending->response);
}

task<std::vector<char>> rpc_call(send_conn_state &state, char *body,
                                 size_t len) {
  auto pending = new pending_call;
  auto t = send_rpc_request(*state.conn, state.send_buf, state.pending_send_buf,
                            state.sending, state.closed, body, len, pending);
  *reinterpret_cast<uint64_t *>(body) = reinterpret_cast<uint64_t>(pending);
  pending->h = t.h_;
  pending->h.resume();
  return t;
}

task<std::optional<std::pair<std::vector<char>, std::string_view>>>
remote_query(send_conn_state &state, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto header_body =
      new char[kRPCRequestHeaderSize + key.size() + sizeof(uint32_t)];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::query;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method) + sizeof(uint32_t)) = key.size();
  std::copy_n(key.data(), key.size(),
              header_body + kRPCRequestHeaderSize + sizeof(uint32_t));
  auto response =
      co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
  if (response.empty()) {
    co_return std::nullopt;
  }
  auto p = response.data();
  auto value_size = *reinterpret_cast<uint32_t *>(p);
  p += sizeof(value_size);
  co_return std::make_pair(std::move(response),
                           std::string_view(p, value_size));
}

task<void> remote_add(send_conn_state &state, std::string_view key,
                      std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::add;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
}

task<void> remote_del(send_conn_state &state, std::string_view key) {
  auto body_size = key.size() + sizeof(uint32_t);
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::del;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
}

task<void> remote_batch(send_conn_state &state,
                        std::vector<key_value_view> const &kvs) {
  auto body_size = sizeof(uint32_t);
  for (auto const &kv : kvs) {
    body_size += sizeof(uint32_t) * 2 + kv.key.size() + kv.value.size();
  }
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::batch;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = kvs.size();
  body_p += sizeof(uint32_t);
  for (auto const &kv : kvs) {
    *reinterpret_cast<uint32_t *>(body_p) = kv.key.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.key.data(), kv.key.size(), body_p);
    body_p += kv.key.size();
    *reinterpret_cast<uint32_t *>(body_p) = kv.value.size();
    body_p += sizeof(uint32_t);
    std::copy_n(kv.value.data(), kv.value.size(), body_p);
    body_p += kv.value.size();
  }
  co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
}

task<std::pair<std::vector<char>, std::vector<key_value_view>>>
remote_list(send_conn_state &state,
            std::unordered_set<std::string_view> const &keys) {
  auto body_size = sizeof(uint32_t);
  for (auto const &key : keys) {
    body_size += sizeof(uint32_t) + key.size();
  }
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::list;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = keys.size();
  body_p += sizeof(uint32_t);
  for (auto const &key : keys) {
    *reinterpret_cast<uint32_t *>(body_p) = key.size();
    body_p += sizeof(uint32_t);
    std::copy_n(key.data(), key.size(), body_p);
    body_p += key.size();
  }
  auto response =
      co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
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

task<void> remote_zadd(send_conn_state &state, std::string_view key,
                       std::string_view value, uint32_t score) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 3;
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::zadd;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  body_p += value.size();
  *reinterpret_cast<uint32_t *>(body_p) = score;
  co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
}

task<std::optional<std::pair<std::vector<char>, std::vector<score_value_view>>>>
remote_zrange(send_conn_state &state, std::string_view key, uint32_t min_score,
              uint32_t max_score) {
  auto body_size = sizeof(uint32_t) * 3 + key.size();
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::zrange;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = min_score;
  body_p += sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(body_p) = max_score;
  auto response =
      co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
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

task<void> remote_zrmv(send_conn_state &state, std::string_view key,
                       std::string_view value) {
  auto body_size = key.size() + value.size() + sizeof(uint32_t) * 2;
  auto header_body = new char[kRPCRequestHeaderSize + body_size];
  *reinterpret_cast<method *>(header_body + sizeof(uint64_t)) = method::zrmv;
  *reinterpret_cast<uint32_t *>(header_body + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = header_body + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key.size();
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(state, header_body, kRPCRequestHeaderSize + body_size);
}

task<void> rpc_reply_recv_loop(fstackco::tcp_connection &rpc_conn) {
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
        if (response.writable() < rep_size) {
          response.expand(rep_size - response.writable());
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
      auto n =
          co_await rpc_conn.read(response.write_data(), response.writable());
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

task<void> connect_rpc_client(uint16_t port);

task<void> rpc_server() {
  size_t rpc_conn_id = 0;
  auto listener = fstackco::tcp_listener(main_loop, 58080);
  fmt::print("Starting RPC server\n");
  connect_rpc_client(58080).detach();
  while (true) {
    auto conns = co_await listener.accept();
    for (auto &&conn : conns) {
      fmt::print("New RPC connection {}\n", rpc_conn_id);
      conn.set_loop(rpc_loops[rpc_conn_id % rpc_loops.size()]);
      handle_rpc(std::move(conn), rpc_conn_id);
      rpc_conn_id++;
    }
  }
  co_return;
}

task<void> handle_http_connection(fstackco::tcp_connection conn,
                                  size_t conn_id) {
  auto conn_shard = conn_id % loops.size();
  auto loop = loops[conn_shard];
  co_await io_queues[conn_shard].queue_in_loop(event_fds[conn_shard]);
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
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     HTTP_400, sizeof(HTTP_400) - 1);
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
            if (content_length > request.writable()) {
              request.expand(content_length - request.writable());
            }
          }
        } else if (request.writable() == 0) {
          request.expand(2048);
        }
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
        n = co_await conn.read(request.write_data(), request.writable());
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
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'i': {
          // init
          if (store->kv_loaded()) {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
          } else {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     LOADING_RESPONSE, sizeof(LOADING_RESPONSE) - 1);
          }
        } break;
        case 'q': {
          // query
          std::string_view key(path + 7, path_len - 7);
          std::pair<char *, size_t> value;
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            value = store->query(key);
          } else {
            auto remote_value = co_await remote_query(
                rpc_clients[key_shard][conn_shard].get_conn(), key);
            if (!remote_value.has_value()) {
              value.first = nullptr;
            } else {
              value.first = new char[remote_value.value().second.size()];
              value.second = remote_value.value().second.size();
              std::copy_n(remote_value.value().second.data(),
                          remote_value.value().second.size(), value.first);
            }
          }
          if (!value.first) {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            send_text(conn, sending, closed, send_buf, pending_send_buf,
                      value.first, value.second);
          }
        } break;
        case 'd': {
          // del
          std::string_view key(path + 5, path_len - 5);
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            store->del(key);
          } else {
            co_await remote_del(rpc_clients[key_shard][conn_shard].get_conn(),
                                key);
          }
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
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
            co_await remote_zrmv(rpc_clients[key_shard][conn_shard].get_conn(),
                                 key, value);
          }
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        }
      } break;
      case 'P': {
        switch (path[1]) {
        default: {
          send_all(conn, sending, closed, send_buf, pending_send_buf, HTTP_404,
                   sizeof(HTTP_404) - 1);
        } break;
        case 'u': {
          // updateCluster
          std::string_view clusterjson(body_start, body_received);
          simdjson::padded_string padded(clusterjson);
          auto doc = parser.parse(padded);
          nr_peers = doc["hosts"].get_array().size();
          me = doc["index"].get_uint64() - 1;
          {
            std::ofstream ofs("/data/cluster.json");
            ofs << clusterjson;
          }
          {
            bool init = false;
            if (peers_updated.compare_exchange_strong(init, true)) {
              co_await connect_rpc_client(58080);
              co_await io_queues[conn_shard].queue_in_loop(
                  event_fds[conn_shard]);
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
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'a': {
          // add
          simdjson::padded_string json =
              simdjson::padded_string(body_start, content_length);
          auto doc = parser.parse(json);
          auto const key = doc["key"].get_string().take_value();
          auto const value = doc["value"].get_string().take_value();
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            store->add(key, value);
          } else {
            co_await remote_add(rpc_clients[key_shard][conn_shard].get_conn(),
                                key, value);
          }
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'b': {
          // batch
          simdjson::padded_string json =
              simdjson::padded_string(body_start, content_length);
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
            tasks.emplace_back(
                remote_batch(rpc_clients[key_shard][conn_shard].get_conn(),
                             sharded_keys[key_shard]));
          }
          for (auto &&task : tasks) {
            co_await task;
          }
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'l': {
          // list
          simdjson::padded_string json =
              simdjson::padded_string(body_start, content_length);
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::unordered_set<std::string_view> keys;
          std::vector<std::unordered_set<std::string_view>> sharded_keys(
              nr_peers);
          std::vector<std::vector<key_value_view>> remote_key_values;
          for (auto &&k : arr) {
            auto key = k.get_string().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              keys.insert(key);
            } else {
              sharded_keys[key_shard].insert(key);
            }
          }
          auto remote_kv_count = 0;
          std::vector<
              task<std::pair<std::vector<char>, std::vector<key_value_view>>>>
              tasks;
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            tasks.emplace_back(
                remote_list(rpc_clients[key_shard][conn_shard].get_conn(),
                            sharded_keys[key_shard]));
          }
          auto local_key_values = store->list(keys.begin(), keys.end());
          for (auto &t : tasks) {
            auto remote_kvs = co_await t;
            remote_kv_count += remote_kvs.second.size();
            remote_key_values.emplace_back(std::move(remote_kvs.second));
          }
          if (local_key_values.empty() && remote_kv_count == 0) {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            json_output_stream stream;
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
              for (auto &remote_kvs : remote_key_values) {
                for (auto &kv : remote_kvs) {
                  write_kv(kv.key, kv.value);
                }
              }
              rapidjson::Writer<json_output_stream> writer(stream);
              d.Accept(writer);
            }
            send_json(conn, sending, closed, send_buf, pending_send_buf,
                      stream);
          }
        } break;
        case 'z': {
          switch (path[2]) {
          case 'a': {
            // zadd
            std::string_view key(&path[6], path_len - 6);
            simdjson::padded_string json =
                simdjson::padded_string(body_start, content_length);
            auto score_value = parser.parse(json);
            auto value = score_value["value"].get_string().take_value();
            auto score = score_value["score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              store->zadd(key, value, score);
            } else {
              co_await remote_zadd(
                  rpc_clients[key_shard][conn_shard].get_conn(), key, value,
                  score);
            }
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          } break;
          case 'r': {
            // zrange
            std::string_view key(&path[8], path_len - 8);
            simdjson::padded_string json =
                simdjson::padded_string(body_start, content_length);
            auto score_range = parser.parse(json);
            auto min_score = score_range["min_score"].get_uint64().take_value();
            auto max_score = score_range["max_score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            json_output_stream stream;
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
            if (key_shard == me) {
              auto score_values = store->zrange(key, min_score, max_score);
              if (!score_values.has_value()) {
                send_all(conn, sending, closed, send_buf, pending_send_buf,
                         HTTP_404, sizeof(HTTP_404) - 1);
              } else {
                for (auto &sv : score_values.value()) {
                  write_sv(sv.value, sv.score);
                }
                rapidjson::Writer<json_output_stream> writer(stream);
                d.Accept(writer);
                send_json(conn, sending, closed, send_buf, pending_send_buf,
                          stream);
              }
            } else {
              auto score_values = co_await remote_zrange(
                  rpc_clients[key_shard][conn_shard].get_conn(), key, min_score,
                  max_score);
              if (!score_values.has_value()) {
                send_all(conn, sending, closed, send_buf, pending_send_buf,
                         HTTP_404, sizeof(HTTP_404) - 1);
              } else {
                for (auto &sv : score_values.value().second) {
                  write_sv(sv.value, sv.score);
                }
                rapidjson::Writer<json_output_stream> writer(stream);
                d.Accept(writer);
                send_json(conn, sending, closed, send_buf, pending_send_buf,
                          stream);
              }
            }
          } break;
          }
        } break;
        }
      } break;
      default:
        send_all(conn, sending, closed, send_buf, pending_send_buf, OK_RESPONSE,
                 sizeof(OK_RESPONSE) - 1);
      }
      request.advance_read(parser_rc + content_length);
      content_length = 0;
      body_received = 0;
      num_headers = 16;
      parser_rc = -2;
    }
  } catch (std::exception &e) {
    fmt::print("conn {} error {}\n", conn.fd().fd(), e.what());
  }

  co_return;
}

task<void> http_server() {
  size_t conn_id = 0;
  auto listener = fstackco::tcp_listener(main_loop, 8080);
  for (;;) {
    auto conns = co_await listener.accept();
    for (auto &&conn : conns) {
      conn.set_loop(loops[conn_id % loops.size()]);
      handle_http_connection(std::move(conn), conn_id).detach();
      conn_id++;
    }
  }
}

int main(int argc, char *argv[]) {
  fstackco::init(argc, argv);
  main_loop = std::make_shared<fstackco::event_loop>();
  main_loop->loop();
  return 0;
}