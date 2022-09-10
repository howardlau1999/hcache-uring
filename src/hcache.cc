#include "io_buffer.h"
#include "storage.h"
#include "threading.h"
#include <algorithm>
#include <array>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <fmt/format.h>
#include <memory>
#include <picohttpparser/picohttpparser.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>
#include <string>
#include <strings.h>
#include <sys/socket.h>
#include <uringpp/event_loop.h>
#include <uringpp/file.h>
#include <uringpp/listener.h>
#include <uringpp/socket.h>
#include <uringpp/task.h>

using uringpp::task;

std::vector<std::shared_ptr<loop_with_queue>> loops;
std::atomic<size_t> loop_started;
std::unique_ptr<storage> store;

const char HTTP_404[] = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
const char HTTP_400[] = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
const char OK_RESPONSE[] = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
const char EMPTY_RESPONSE[] = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

auto format_200_header(size_t body_length) {
  return fmt::format("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                     body_length);
}

task<void> send_worker(uringpp::socket &conn, io_buffer &buffer,
                       io_buffer &pending, bool &sending, bool &closed) {
  sending = true;
  while (buffer.readable() > 0) {
    auto n = co_await conn.send(buffer.read_data(), buffer.readable());
    if (n <= 0) {
      closed = true;
      co_return;
    }
    buffer.advance_read(n);
    if (buffer.readable() == 0 && pending.readable() > 0) {
      std::swap(buffer, pending);
    }
  }
  sending = false;
}

void send_all(uringpp::socket &conn, bool &sending, bool &closed,
              io_buffer &send_buf, io_buffer &pending_send_buf,
              char const *data, size_t len) {
  if (closed)
    return;
  if (sending) {
    if (pending_send_buf.writable() < len) {
      pending_send_buf.expand(len - pending_send_buf.writable());
    }
    std::copy_n(data, len, pending_send_buf.write_data());
    pending_send_buf.advance_write(len);
  } else {
    if (send_buf.writable() < len) {
      send_buf.expand(len - send_buf.writable());
    }
    std::copy_n(data, len, send_buf.write_data());
    send_buf.advance_write(len);
    send_worker(conn, send_buf, pending_send_buf, sending, closed).detach();
  }
}

void send_json(uringpp::socket &conn, bool &sending, bool &closed,
               io_buffer &send_buf, io_buffer &pending_send_buf,
               rapidjson::StringBuffer const &buffer) {
  {
    auto header = format_200_header(buffer.GetSize());
    send_all(conn, sending, closed, send_buf, pending_send_buf, header.data(),
             header.size());
  }
  send_all(conn, sending, closed, send_buf, pending_send_buf,
           buffer.GetString(), buffer.GetSize());
}

void send_text(uringpp::socket &conn, bool &sending, bool &closed,
               io_buffer &send_buf, io_buffer &pending_send_buf,
               std::string const &buffer) {
  {
    auto header = format_200_header(buffer.size());
    send_all(conn, sending, closed, send_buf, pending_send_buf, header.data(),
             header.size());
  }
  send_all(conn, sending, closed, send_buf, pending_send_buf, buffer.data(),
           buffer.size());
}

constexpr int kStateRecvHeader = 0;
constexpr int kStateRecvBody = 1;

task<void> handle_rpc(uringpp::socket conn, size_t conn_id) {
  auto loop = loops[conn_id % loops.size()];
  co_await loop->switch_to_io_thread();
  std::array<char, 4096> buffer;
  int state = kStateRecvHeader;
  size_t body_length = 0;
  size_t body_read = 0;
  size_t header_read = 0;
  io_buffer request(4096);
  while (true) {
  }
  co_return;
}

task<void> rpc_server(std::shared_ptr<uringpp::event_loop> loop) {
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", "58080");
  while (true) {
    auto [addr, conn] = co_await listener.accept();
  }
  co_return;
}

task<void> handle_http(uringpp::socket conn, size_t conn_id) {
  auto loop = loops[conn_id % loops.size()];
  co_await loop->switch_to_io_thread();
  try {
    std::array<char, 4096> buffer;
    io_buffer request(4096);
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
    io_buffer send_buf(4096);
    io_buffer pending_send_buf(4096);
    phr_header headers[16];
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
          }
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
        n = co_await conn.recv(&buffer[0], buffer.size(), MSG_NOSIGNAL);
        if (n <= 0) {
          co_return;
        }
        if (request.writable() < n) {
          request.expand(n - request.writable());
        }
        std::copy_n(&buffer[0], n, request.write_data());
        request.advance_write(n);
        continue;
      }

      switch (method[0]) {
      case 'G': {
        switch (path[1]) {
        default:
        case 'i': {
          // init
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'q': {
          // query
          std::string_view key(path + 7, path_len - 7);
          auto value = store->query(key);
          if (!value.has_value()) {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            send_text(conn, sending, closed, send_buf, pending_send_buf,
                      value.value());
          }
        } break;
        case 'd': {
          // del
          std::string_view key(path + 5, path_len - 5);
          store->del(key);
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
          store->zrmv(key, value);
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
          auto f = co_await uringpp::file::open(loop, "/data/cluster.json",
                                                O_CREAT | O_RDWR, 0644);
          co_await f.write(body_start, content_length, 0);
          co_await f.close();
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'a': {
          // add
          auto document = rapidjson::Document();
          document.Parse(body_start, content_length);
          auto const key = std::string_view(document["key"].GetString(),
                                            document["key"].GetStringLength());
          auto const value =
              std::string_view(document["value"].GetString(),
                               document["value"].GetStringLength());
          store->add(key, value);
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'b': {
          // batch
          auto document = rapidjson::Document();
          document.Parse(body_start, content_length);
          auto arr = document.GetArray();
          for (auto &&kv : arr) {
            auto const key = std::string_view(kv["key"].GetString(),
                                              kv["key"].GetStringLength());
            auto const value = std::string_view(kv["value"].GetString(),
                                                kv["value"].GetStringLength());
            store->add(key, value);
          }
          send_all(conn, sending, closed, send_buf, pending_send_buf,
                   EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
        } break;
        case 'l': {
          // list
          auto document = rapidjson::Document();
          document.Parse(body_start, content_length);
          auto arr = document.GetArray();
          std::unordered_set<std::string_view> keys;
          for (auto &&key : arr) {
            keys.emplace(key.GetString(), key.GetStringLength());
          }
          auto key_values = store->list(keys.begin(), keys.end());
          if (key_values.empty()) {
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            rapidjson::StringBuffer buffer;
            {
              auto d = rapidjson::Document();
              auto &kv_list = d.SetArray();
              auto &allocator = d.GetAllocator();
              auto write_kv = [&](std::string_view key,
                                  std::string_view value) {
                auto &&kv_object = rapidjson::Value(rapidjson::kObjectType);
                auto &&k_string = rapidjson::StringRef(key.data(), key.size());
                auto &&v_string =
                    rapidjson::StringRef(value.data(), value.size());
                kv_object.AddMember("key", k_string, allocator);
                kv_object.AddMember("value", v_string, allocator);
                kv_list.PushBack(kv_object, allocator);
              };
              for (auto &kv : key_values) {
                write_kv(kv.key, kv.value);
              }
              rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
              d.Accept(writer);
            }
            send_json(conn, sending, closed, send_buf, pending_send_buf,
                      buffer);
          }
        } break;
        case 'z': {
          switch (path[2]) {
          case 'a': {
            // zadd
            std::string_view key(&path[6], path_len - 6);
            auto score_value = rapidjson::Document();
            score_value.Parse(body_start, content_length);
            auto value =
                std::string_view(score_value["value"].GetString(),
                                 score_value["value"].GetStringLength());
            auto score = score_value["score"].GetUint64();
            store->zadd(key, value, score);
            send_all(conn, sending, closed, send_buf, pending_send_buf,
                     EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          } break;
          case 'r': {
            // zrange
            std::string_view key(&path[8], path_len - 8);
            auto score_range = rapidjson::Document();
            score_range.Parse(body_start, content_length);
            auto min_score = score_range["min_score"].GetUint64();
            auto max_score = score_range["max_score"].GetUint64();
            auto score_values = store->zrange(key, min_score, max_score);
            if (!score_values.has_value()) {
              send_all(conn, sending, closed, send_buf, pending_send_buf,
                       HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              rapidjson::StringBuffer buffer;
              {
                auto d = rapidjson::Document();
                auto &sv_list = d.SetArray();
                auto &allocator = d.GetAllocator();
                for (auto &kv : score_values.value()) {
                  auto &&sv_object = rapidjson::Value(rapidjson::kObjectType);
                  auto &&v_string =
                      rapidjson::StringRef(kv.value.data(), kv.value.size());
                  sv_object.AddMember("value", v_string, allocator);
                  sv_object.AddMember("score", kv.score, allocator);
                  sv_list.PushBack(sv_object, allocator);
                }
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                d.Accept(writer);
              }
              send_json(conn, sending, closed, send_buf, pending_send_buf,
                        buffer);
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
    fmt::print("conn {} error {}\n", conn.fd(), e.what());
  }

  co_return;
}

task<void> http_server(std::shared_ptr<uringpp::event_loop> loop) {
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", "8080");
  size_t conn_id = 0;
  while (true) {
    try {
      auto [addr, conn] =
          co_await listener.accept(loops[conn_id % loops.size()]);
      handle_http(std::move(conn), conn_id).detach();
      conn_id++;
    } catch (std::exception &e) {
      fmt::print("Failed to accept {}\n", e.what());
    }
  }
}

int main() {
  auto main_loop = uringpp::event_loop::create();
  // ::signal(SIGPIPE, SIG_IGN);
  auto cores = get_cpu_affinity();
  loops.resize(cores.size());
  store = std::make_unique<storage>();
  store->load_kv();
  store->load_zset();
  bind_cpu(cores[0]);
  for (int i = 0; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i], main_loop] {
      fmt::print("thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      auto loop = loop_with_queue::create(1024, IORING_SETUP_ATTACH_WQ,
                                          main_loop->fd());
      loops[i] = loop;
      loop_started.fetch_add(1);
      for (;;) {
        loop->run_pending();
        loop->poll_no_wait();
      }
    });
    thread.detach();
  }
  while (loop_started.load() != cores.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  main_loop->block_on(http_server(main_loop));
  return 0;
}