#include "cds/init.h"
#include "fmt/core.h"
#include "io_buffer.h"
#include "rapidjson/stringbuffer.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "rpc.h"
#include "storage.h"
#include "threading.h"
#include <algorithm>
#include <array>
#include <bits/types/struct_iovec.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <csignal>
#include <cstdint>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <fmt/format.h>
#include <liburing/io_uring.h>
#include <linux/time_types.h>
#include <memory>
#include <mutex>
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
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <thread>
#include <unordered_set>
#include <uringpp/event_loop.h>
#include <uringpp/file.h>
#include <uringpp/listener.h>
#include <uringpp/socket.h>
#include <uringpp/task.h>
#include <vector>

using uringpp::task;
using namespace std::chrono_literals;

rocksdb::WriteOptions write_options;

void init_load();
task<void> send_all(uringpp::socket &conn, const char *data, size_t size);

std::vector<char> encode_zset_key(std::string_view key,
                                  std::string_view value) {
  std::vector<char> full_key(key.size() + value.size() + 1);
  std::copy(key.begin(), key.end(), full_key.begin());
  full_key[key.size()] = '\0';
  std::copy(value.begin(), value.end(), full_key.begin() + key.size() + 1);
  return full_key;
}

std::pair<std::string_view, std::string_view>
decode_zset_key(rocksdb::Slice const &full_key) {
  size_t zero_pos = 0;
  for (size_t i = 0; i < full_key.size(); ++i) {
    if (full_key[i] == '\0') {
      zero_pos = i;
      break;
    }
  }
  return {std::string_view(full_key.data(), zero_pos),
          std::string_view(full_key.data() + zero_pos + 1,
                           full_key.size() - zero_pos - 1)};
}

const std::chrono::milliseconds task_quota = 10ms;

std::chrono::steady_clock::time_point now() {
  return std::chrono::steady_clock::now();
}

constexpr bool
exceed_quota(std::chrono::steady_clock::time_point const &start) {
  // return now() - start > task_quota;
  return false;
}

template <size_t align> constexpr size_t align_up(const uint64_t size) {
  static_assert((align & (align - 1)) == 0, "align must be power of 2");
  return (size + align - 1) & ~(align - 1);
}

struct send_conn_state {
  std::unique_ptr<uringpp::socket> conn;
};

task<void> rpc_reply_recv_loop(uringpp::socket &rpc_conn);

struct conn_pool {
  static constexpr size_t kMaxConns = 1024;
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
std::vector<std::vector<conn_pool>> rpc_clients;
std::atomic<size_t> loop_started = 0;
std::atomic<size_t> clients_connected = 0;
std::shared_ptr<loop_with_queue> main_loop;
std::vector<std::string> peer_hosts;
std::atomic<bool> peers_updated = false;
std::vector<int> cores;
size_t nr_peers = 1;
size_t me = 0;

thread_local size_t shard_id;
thread_local unordered_string_map<std::string> *kvs_;
thread_local unordered_string_map<std::unique_ptr<zset_stl>> *zsets_;
thread_local rocksdb::DB *kv_db_;
thread_local rocksdb::DB *zset_db_;
std::vector<io_queue *> persist_queue_;
std::vector<int> persist_efds_;
std::vector<unordered_string_map<std::string> *> kv_shards_;
std::vector<unordered_string_map<std::unique_ptr<zset_stl>> *> zset_shards_;
std::vector<std::mutex *> init_locks_;
std::vector<rocksdb::DB *> kv_db_shards_;
std::vector<rocksdb::DB *> zset_db_shards_;
std::atomic<bool> kv_initializing_;
std::atomic<bool> kv_initialized_;

size_t get_shard(std::string_view key) {
  auto hash = XXH64(key.data(), key.size(), 19260817);
  return hash % nr_peers;
}

size_t get_core(std::string_view key) {
  auto hash = XXH64(key.data(), key.size(), 19990315);
  return hash % cores.size();
}

uringpp::task<std::optional<std::string>> xcore_query(std::string_view key) {
  auto core = get_core(key);
  if (core == shard_id) {
    auto it = kvs_->find(key);
    if (it != kvs_->end()) {
      co_return it->second;
    }
    co_return std::nullopt;
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  auto ret = [&]() -> std::optional<std::string> {
    auto it = kvs_->find(key);
    if (it != kvs_->end()) {
      return it->second;
    }
    return std::nullopt;
  }();
  co_await loops[original_shard_id]->switch_to_io_thread(core);
  co_return ret;
}

task<void> delay_add_persist(std::string key, std::string value) {
  auto kv_db = kv_db_shards_[shard_id];
  co_await persist_queue_[shard_id]->queue_in_loop();
  kv_db->Put(rocksdb::WriteOptions(), key, value);
}

void xcore_add_local_no_persist(std::string_view key, std::string_view value) {
  kvs_->insert({std::string(key), std::string(value)});
}

void xcore_add_local(std::string_view key, std::string_view value) {
  xcore_add_local_no_persist(key, value);
  delay_add_persist(std::string(key), std::string(value)).detach();
}

uringpp::task<void> xcore_add(std::string_view key, std::string_view value) {
  auto core = get_core(key);
  if (core == shard_id) {
    xcore_add_local(key, value);
    co_return;
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  xcore_add_local(key, value);
  co_await loops[original_shard_id]->switch_to_io_thread(core);
}

task<void> delay_del_persist(std::string key) {
  auto kv_db = kv_db_;
  co_await persist_queue_[shard_id]->queue_in_loop();
  kv_db->Delete(rocksdb::WriteOptions(), key);
}

void xcore_del_local(std::string_view key) {
  {
    auto it = kvs_->find(key);
    if (it != kvs_->end()) {
      kvs_->erase(it);
    }
    delay_del_persist(std::string(key)).detach();
  }
  {
    auto it = zsets_->find(key);
    if (it != zsets_->end()) {
      zsets_->erase(it);
    }
  }
}

uringpp::task<void> xcore_del(std::string_view key) {
  auto core = get_core(key);
  if (core == shard_id) {
    xcore_del_local(key);
    co_return;
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  xcore_del_local(key);
  assert(original_shard_id < loops.size());
  assert(core < loops.size());
  co_await loops[original_shard_id]->switch_to_io_thread(core);
}

task<void> delay_zadd_persist(std::vector<char> full_key, uint32_t score) {
  auto zset_db = zset_db_;
  co_await persist_queue_[shard_id]->queue_in_loop();
  zset_db->Put(write_options, rocksdb::Slice(full_key.data(), full_key.size()),
               rocksdb::Slice((char *)&score, sizeof(score)));
}

void xcore_zadd_local_no_persist(std::string_view key, std::string_view value,
                                 uint32_t score) {
  auto it = zsets_->find(key);
  if (it == zsets_->end()) {
    it = zsets_->insert({std::string(key), std::make_unique<zset_stl>()}).first;
  }
  it->second->zadd(score, value);
}

void xcore_zadd_local(std::string_view key, std::string_view value,
                      uint32_t score) {
  xcore_zadd_local_no_persist(key, value, score);
  delay_zadd_persist(encode_zset_key(key, value), score).detach();
}

uringpp::task<void> xcore_zadd(std::string_view key, std::string_view value,
                               uint32_t score) {
  auto core = get_core(key);
  if (core == shard_id) {
    xcore_zadd_local(key, value, score);
    co_return;
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  xcore_zadd_local(key, value, score);
  co_await loops[original_shard_id]->switch_to_io_thread(core);
}

task<void> delay_zrmv_persist(std::vector<char> full_key) {
  auto zset_db = zset_db_;
  co_await persist_queue_[shard_id]->queue_in_loop();
  zset_db->Delete(write_options,
                  rocksdb::Slice(full_key.data(), full_key.size()));
}

void xcore_zrmv_local(std::string_view key, std::string_view value) {
  auto it = zsets_->find(key);
  if (it != zsets_->end()) {
    auto score = it->second->value_score.find(value);
    assert(score != it->second->value_score.end());
    auto &score_values = it->second->score_values[score->second];
    score_values.erase(score_values.find(value));
    it->second->value_score.erase(score);
  }
  delay_zrmv_persist(encode_zset_key(key, value)).detach();
}

uringpp::task<void> xcore_zrmv(std::string_view key, std::string_view value) {
  auto core = get_core(key);
  if (core == shard_id) {
    xcore_zrmv_local(key, value);
    co_return;
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  xcore_zrmv_local(key, value);
  co_await loops[original_shard_id]->switch_to_io_thread(core);
}

uringpp::task<void> xcore_batch_local(std::vector<key_value_view> const &kvs) {
  auto write_batch = rocksdb::WriteBatch();
  auto start = now();
  for (auto const kv : kvs) {
    kvs_->insert({std::string(kv.key), std::string(kv.value)});
    write_batch.Put(kv.key, kv.value);
    if (exceed_quota(start)) {
      co_await loops[shard_id]->switch_to_io_thread(shard_id);
      start = now();
    }
  }
  kv_db_->Write(write_options, &write_batch);
}

uringpp::task<void> xcore_batch_remote(std::vector<key_value_view> const &kvs,
                                       size_t from_shard_id,
                                       size_t target_shard_id) {
  co_await loops[target_shard_id]->switch_to_io_thread(from_shard_id);
  co_await xcore_batch_local(kvs);
  co_await loops[from_shard_id]->switch_to_io_thread(target_shard_id);
}

uringpp::task<void> xcore_batch(std::vector<key_value_view> const &kvs) {
  std::vector<std::vector<key_value_view>> kvs_per_core(cores.size());
  for (auto &kv : kvs) {
    auto core = get_core(kv.key);
    kvs_per_core[core].push_back(kv);
  }
  std::vector<uringpp::task<void>> tasks;
  auto original_shard_id = shard_id;
  for (size_t i = 0; i < cores.size(); ++i) {
    if (i == shard_id) {
      continue;
    } else {
      tasks.push_back(
          xcore_batch_remote(kvs_per_core[i], original_shard_id, i));
    }
  }
  co_await xcore_batch_local(kvs_per_core[shard_id]);
  for (auto &task : tasks) {
    co_await task;
  }
}

uringpp::task<std::vector<key_view_value>>
xcore_list_local(std::vector<std::string_view> const &keys) {
  auto start = now();
  std::vector<key_view_value> result;
  for (auto const key : keys) {
    auto it = kvs_->find(key);
    if (it != kvs_->end()) {
      result.push_back({key, it->second});
    }
    if (exceed_quota(start)) {
      co_await loops[shard_id]->switch_to_io_thread(shard_id);
      start = now();
    }
  }
  co_return result;
}

uringpp::task<std::vector<key_view_value>>
xcore_list_remote(std::vector<std::string_view> const &keys,
                  size_t from_shard_id, size_t target_shard_id) {
  co_await loops[target_shard_id]->switch_to_io_thread(from_shard_id);
  auto result = co_await xcore_list_local(keys);
  co_await loops[from_shard_id]->switch_to_io_thread(target_shard_id);
  co_return result;
}

template <class It>
uringpp::task<std::vector<std::vector<key_view_value>>> xcore_list(It begin,
                                                                   It end) {
  std::vector<std::vector<std::string_view>> keys_per_core(cores.size());
  for (auto it = begin; it != end; ++it) {
    auto core = get_core(*it);
    keys_per_core[core].push_back(*it);
  }
  std::vector<uringpp::task<std::vector<key_view_value>>> tasks;
  auto original_shard_id = shard_id;
  for (size_t i = 0; i < cores.size(); ++i) {
    if (i == shard_id) {
      continue;
    } else {
      tasks.push_back(
          xcore_list_remote(keys_per_core[i], original_shard_id, i));
    }
  }
  std::vector<std::vector<key_view_value>> result;
  result.emplace_back(co_await xcore_list_local(keys_per_core[shard_id]));
  for (auto &task : tasks) {
    auto res = co_await task;
    result.emplace_back(std::move(res));
  }
  co_return result;
}

uringpp::task<void> maybe_yield(std::chrono::steady_clock::time_point &start) {
  if (exceed_quota(start)) {
    co_await loops[shard_id]->switch_to_io_thread(shard_id);
    start = now();
  }
}

uringpp::task<std::optional<std::vector<score_value>>>
xcore_zrange_local(std::string_view key, uint32_t min_score,
                   uint32_t max_score) {
  auto it = zsets_->find(key);
  if (it == zsets_->end()) {
    co_return std::nullopt;
  }
  auto &score_values = it->second->score_values;
  auto start = now();
  std::vector<score_value> result;
  for (auto score = min_score; score <= max_score;) {
    auto score_value_it = score_values.lower_bound(score);
    if (score_value_it == score_values.end()) {
      break;
    }
    while (score_value_it != score_values.end() &&
           score_value_it->first == score) {
      for (auto const &value : score_value_it->second) {
        result.emplace_back(value.key, score);
      }
      ++score_value_it;
      co_await maybe_yield(start);
    }
    if (score_value_it == score_values.end()) {
      break;
    }
    score = score_value_it->first;
    co_await maybe_yield(start);
  }
  co_return result;
}

uringpp::task<std::optional<std::vector<score_value>>>
xcore_zrange(std::string_view key, uint32_t min_score, uint32_t max_score) {
  auto core = get_core(key);
  if (core == shard_id) {
    co_return co_await xcore_zrange_local(key, min_score, max_score);
  }
  auto original_shard_id = shard_id;
  co_await loops[core]->switch_to_io_thread(original_shard_id);
  auto result = co_await xcore_zrange_local(key, min_score, max_score);
  co_await loops[original_shard_id]->switch_to_io_thread(core);
  co_return result;
}

const char HTTP_404[] = "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n";
const char HTTP_400[] = "HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\n\r\n";
const char OK_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
const char EMPTY_RESPONSE[] = "HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
const char EMPTY_ARRAY[] = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n[]";
const char CHUNK_RESPONSE[] =
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n[\r\n";
const char CHUNK_END[] = "1\r\n]\r\n0\r\n\r\n";
const char LOADING_RESPONSE[] =
    "HTTP/1.1 200 OK\r\ncontent-length: 7\r\n\r\nloading";
const char HTTP_200_HEADER[] = "HTTP/1.1 200 OK\r\ncontent-length: ";
const char HEADER_END[] = "\r\n\r\n";

constexpr size_t kMaxWriteSize = 1024 * 1024 * 1024;

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
    if (n <= 0) {
      fmt::print("written {} total {}\n", written, total);
      throw std::runtime_error("writev_all failed");
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

[[nodiscard]] uringpp::task<void> send_text(uringpp::socket &conn,
                                            char const *data, size_t len) {

  char length[8];
  size_t len_chars = itoa(length, len);
  struct iovec iov[4];
  iov[0].iov_base = const_cast<char *>(HTTP_200_HEADER);
  iov[0].iov_len = sizeof(HTTP_200_HEADER) - 1;
  iov[1].iov_base = length;
  iov[1].iov_len = len_chars;
  iov[2].iov_base = const_cast<char *>(HEADER_END);
  iov[2].iov_len = sizeof(HEADER_END) - 1;
  iov[3].iov_base = const_cast<char *>(data);
  iov[3].iov_len = len;
  co_await writev_all(conn, iov, 4);
}

constexpr size_t kRPCReplyHeaderSize = sizeof(uint64_t) + sizeof(uint32_t);

uringpp::task<void> rpc_reply_header(uringpp::socket &conn, uint64_t req_id,
                                     uint32_t len, uint32_t count) {
  char header[kRPCReplyHeaderSize];
  auto p = header;
  *(uint64_t *)p = req_id;
  p += sizeof(uint64_t);
  *(uint32_t *)p = len;
  p += sizeof(uint32_t);
  struct iovec iov[2];
  iov[0].iov_base = header;
  iov[0].iov_len = kRPCReplyHeaderSize;
  iov[1].iov_base = (void *)(&count);
  iov[1].iov_len = sizeof(uint32_t);
  co_await writev_all(conn, iov, 2);
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
  co_await writev_all(conn, iov, 2);
}

constexpr size_t kRPCRequestHeaderSize =
    sizeof(uint64_t) + sizeof(method) + sizeof(uint32_t);

task<void> handle_rpc(uringpp::socket conn, size_t conn_id) {
  auto loop = loops[conn_id % loops.size()];
  co_await loop->switch_to_io_thread(shard_id);
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
      auto n = co_await conn.recv(request.write_data(), request.writable(),
                                  MSG_NOSIGNAL);
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
      uint32_t key_size_align = align_up<sizeof(uint32_t)>(key_size);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size_align;
      auto value = co_await xcore_query(key);
      if (value.has_value()) {
        uint32_t size_u32 = static_cast<uint32_t>(value->size());
        auto rep_body_size =
            align_up<sizeof(uint32_t)>(value->size()) + sizeof(size_u32);
        auto rep_body = std::vector<char>(rep_body_size);
        std::copy_n(reinterpret_cast<char *>(&size_u32), sizeof(size_u32),
                    rep_body.begin());
        std::copy_n(value->data(), value->size(),
                    rep_body.begin() + sizeof(size_u32));
        co_await rpc_reply(conn, req_id, std::move(rep_body));
      } else {
        co_await rpc_reply(conn, req_id, {});
      }
    } break;
    case method::add: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t key_size_align = align_up<sizeof(uint32_t)>(key_size);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size_align;
      uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t value_size_align = align_up<sizeof(uint32_t)>(value_size);
      req_p += sizeof(value_size);
      std::string_view value(req_p, value_size);
      req_p += value_size_align;
      co_await rpc_reply(conn, req_id, {});
      co_await xcore_add(key, value);
    } break;
    case method::del: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size;
      co_await rpc_reply(conn, req_id, {});
      co_await xcore_del(key);
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
      auto values = co_await xcore_list(keys.begin(), keys.end());
      auto total_count = 0;
      size_t rep_body_size = sizeof(uint32_t);
      for (auto &v : values) {
        total_count += v.size();
        for (auto const kv : v) {
          rep_body_size +=
              sizeof(uint32_t) * 2 + kv.key.size() + kv.value.size();
        }
      }
      co_await rpc_reply_header(conn, req_id, rep_body_size, total_count);
      constexpr size_t batch_size = 256;
      uint32_t sizes[batch_size * 2];
      struct iovec iov[batch_size * 4];
      size_t batch_idx = 0;
      for (auto const &vkv : values) {
        for (auto const &kv : vkv) {
          sizes[batch_idx * 2] = kv.key.size();
          sizes[batch_idx * 2 + 1] = kv.value.size();
          iov[batch_idx * 4].iov_base = &sizes[batch_idx * 2];
          iov[batch_idx * 4].iov_len = sizeof(uint32_t);
          iov[batch_idx * 4 + 1].iov_base = const_cast<char *>(kv.key.data());
          iov[batch_idx * 4 + 1].iov_len = kv.key.size();
          iov[batch_idx * 4 + 2].iov_base = &sizes[batch_idx * 2 + 1];
          iov[batch_idx * 4 + 2].iov_len = sizeof(uint32_t);
          iov[batch_idx * 4 + 3].iov_base = const_cast<char *>(kv.value.data());
          iov[batch_idx * 4 + 3].iov_len = kv.value.size();
          if (++batch_idx == batch_size) {
            co_await writev_all(conn, iov, batch_idx * 4);
            batch_idx = 0;
          }
        }
      }
      if (batch_idx > 0) {
        co_await writev_all(conn, iov, batch_idx * 4);
      }
    } break;
    case method::batch: {
      uint32_t count = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(count);
      std::vector<key_value_view> kvs(count);
      for (uint32_t i = 0; i < count; i++) {
        uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(key_size);
        std::string_view key(req_p, key_size);
        req_p += key_size;
        uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
        req_p += sizeof(value_size);
        std::string_view value(req_p, value_size);
        req_p += value_size;
        kvs[i] = {key, value};
      }
      co_await rpc_reply(conn, req_id, {});
      co_await xcore_batch(kvs);
    } break;
    case method::zadd: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t key_size_align = align_up<sizeof(uint32_t)>(key_size);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size_align;
      uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t value_size_align = align_up<sizeof(uint32_t)>(value_size);
      req_p += sizeof(value_size);
      std::string_view value(req_p, value_size);
      req_p += value_size_align;
      uint32_t score = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(score);
      co_await rpc_reply(conn, req_id, {});
      co_await xcore_zadd(key, value, score);
    } break;
    case method::zrmv: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t key_size_align = align_up<sizeof(uint32_t)>(key_size);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size_align;
      uint32_t value_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t value_size_align = align_up<sizeof(uint32_t)>(value_size);
      req_p += sizeof(value_size);
      std::string_view value(req_p, value_size);
      req_p += value_size_align;
      co_await rpc_reply(conn, req_id, {});
      co_await xcore_zrmv(key, value);
    } break;
    case method::zrange: {
      uint32_t key_size = *reinterpret_cast<uint32_t *>(req_p);
      uint32_t key_size_align = align_up<sizeof(uint32_t)>(key_size);
      req_p += sizeof(key_size);
      std::string_view key(req_p, key_size);
      req_p += key_size_align;
      uint32_t min_score = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(min_score);
      uint32_t max_score = *reinterpret_cast<uint32_t *>(req_p);
      req_p += sizeof(max_score);
      auto values = co_await xcore_zrange(key, min_score, max_score);
      if (!values.has_value()) {
        co_await rpc_reply(conn, req_id, {});
      } else {
        auto const &score_values = values.value();
        size_t rep_body_size = sizeof(uint32_t);
        for (auto &v : score_values) {
          rep_body_size += sizeof(uint32_t) * 2 + v.value.size();
        }
        co_await rpc_reply_header(conn, req_id, rep_body_size,
                                  score_values.size());
        rep_body_size -= sizeof(uint32_t);
        std::vector<char> rep_body(rep_body_size);
        auto p = &rep_body[0];
        for (auto &sv : score_values) {
          *reinterpret_cast<uint32_t *>(p) = sv.value.size();
          p += sizeof(uint32_t);
          memcpy(p, sv.value.data(), sv.value.size());
          p += sv.value.size();
          *reinterpret_cast<uint32_t *>(p) = sv.score;
          p += sizeof(uint32_t);
        }
        co_await send_all(conn, rep_body.data(), rep_body_size);
        // constexpr size_t batch_size = 340;
        // struct iovec iov[batch_size * 3];
        // size_t batch_idx = 0;
        // uint32_t sizes[batch_size];
        // for (auto const &sv : score_values) {
        //   sizes[batch_idx] = sv.value.size();
        //   iov[batch_idx * 3].iov_base = &sizes[batch_idx];
        //   iov[batch_idx * 3].iov_len = sizeof(uint32_t);
        //   iov[batch_idx * 3 + 1].iov_base = (void *)sv.value.data();
        //   iov[batch_idx * 3 + 1].iov_len = sv.value.size();
        //   iov[batch_idx * 3 + 2].iov_base = (void *)&sv.score;
        //   iov[batch_idx * 3 + 2].iov_len = sizeof(uint32_t);
        //   if (++batch_idx == batch_size) {
        //     co_await writev_all(conn, iov, batch_idx * 3);
        //     batch_idx = 0;
        //   }
        // }
        // if (batch_idx > 0) {
        //   co_await writev_all(conn, iov, batch_idx * 3);
        // }
      }
    } break;
    }
    request.advance_read(req_size);
    state = kStateRecvHeader;
    req_size = 0;
    req_id = 0;
  }
}

struct pending_call {
  std::coroutine_handle<> h;
  std::vector<char> response;
};

task<std::vector<char>> send_rpc_request(conn_pool &pool,
                                         std::vector<char> body, size_t len,
                                         pending_call *pending) {
  auto conn = co_await pool.get_conn();
  co_await send_all(*conn->conn, body.data(), len);
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
  auto body_size = align_up<sizeof(uint32_t)>(key.size()) + sizeof(uint32_t);
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
  auto key_size_align = align_up<sizeof(uint32_t)>(key.size());
  auto value_size_align = align_up<sizeof(uint32_t)>(value.size());
  auto body_size = key_size_align + value_size_align + sizeof(uint32_t) * 2;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::add;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key_size_align;
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<void> remote_del(conn_pool &pool, std::string_view key) {
  auto body_size = align_up<sizeof(uint32_t)>(key.size()) + sizeof(uint32_t);
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

task<void> send_batch_kv(send_conn_state *state, conn_pool &pool,
                         std::vector<key_value_view> const &kvs,
                         uint32_t body_size) {
  // constexpr size_t batch_size = 256;
  // uint32_t sizes[batch_size * 2];
  // struct iovec iov[batch_size * 4];
  // size_t batch_idx = 0;
  // for (auto const kv : kvs) {
  //   sizes[batch_idx * 2] = kv.key.size();
  //   sizes[batch_idx * 2 + 1] = kv.value.size();
  //   iov[batch_idx * 4].iov_base = (void *)&sizes[batch_idx * 2];
  //   iov[batch_idx * 4].iov_len = sizeof(uint32_t);
  //   iov[batch_idx * 4 + 1].iov_base = (void *)kv.key.data();
  //   iov[batch_idx * 4 + 1].iov_len = kv.key.size();
  //   iov[batch_idx * 4 + 2].iov_base = (void *)&sizes[batch_idx * 2 + 1];
  //   iov[batch_idx * 4 + 2].iov_len = sizeof(uint32_t);
  //   iov[batch_idx * 4 + 3].iov_base = (void *)kv.value.data();
  //   iov[batch_idx * 4 + 3].iov_len = kv.value.size();
  //   if (++batch_idx == batch_size) {
  //     co_await writev_all(*state->conn, iov, batch_idx * 4);
  //     batch_idx = 0;
  //   }
  // }
  // if (batch_idx > 0) {
  //   co_await writev_all(*state->conn, iov, batch_idx * 4);
  // }
  {
    std::vector<char> body(body_size);
    auto p = &body[0];
    for (auto const kv : kvs) {
      *reinterpret_cast<uint32_t *>(p) = kv.key.size();
      p += sizeof(uint32_t);
      std::copy_n(kv.key.data(), kv.key.size(), p);
      p += kv.key.size();
      *reinterpret_cast<uint32_t *>(p) = kv.value.size();
      p += sizeof(uint32_t);
      std::copy_n(kv.value.data(), kv.value.size(), p);
      p += kv.value.size();
    }
    co_await send_all(*state->conn, body.data(), body_size);
  }
  pool.put_conn(state);
  co_await std::suspend_always{};
}

task<void> remote_batch(conn_pool &pool,
                        std::vector<key_value_view> const &kvs) {
  auto body_size = sizeof(uint32_t);
  for (auto const kv : kvs) {
    body_size += sizeof(uint32_t) * 2 + kv.key.size() + kv.value.size();
  }
  auto pending = new pending_call;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize);
  *reinterpret_cast<uint64_t *>(&header_body[0]) =
      reinterpret_cast<uint64_t>(pending);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::batch;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  struct iovec iov[2];
  uint32_t count = kvs.size();
  iov[0].iov_base = (void *)&header_body[0];
  iov[0].iov_len = kRPCRequestHeaderSize;
  iov[1].iov_base = (void *)&count;
  iov[1].iov_len = sizeof(count);
  auto conn = co_await pool.get_conn();
  co_await writev_all(*conn->conn, iov, 2);
  auto t = send_batch_kv(conn, pool, kvs, body_size - sizeof(count));
  pending->h = t.h_;
  co_await t;
}

task<void> send_list_keys(send_conn_state *state, conn_pool &pool,
                          std::unordered_set<std::string_view> const &keys,
                          uint32_t body_size) {
  // constexpr size_t batch_size = 512;
  // struct iovec iov[batch_size * 2];
  // uint32_t sizes[batch_size];
  // size_t batch_idx = 0;
  // for (auto const k : keys) {
  //   sizes[batch_idx] = k.size();
  //   iov[batch_idx * 2].iov_base = &sizes[batch_idx];
  //   iov[batch_idx * 2].iov_len = sizeof(sizes[batch_idx]);
  //   iov[batch_idx * 2 + 1].iov_base = const_cast<char *>(k.data());
  //   iov[batch_idx * 2 + 1].iov_len = k.size();
  //   if (++batch_idx == batch_size) {
  //     co_await writev_all(*state->conn, iov, batch_idx * 2);
  //     batch_idx = 0;
  //   }
  // }
  // if (batch_idx > 0) {
  //   co_await writev_all(*state->conn, iov, batch_idx * 2);
  // }
  {
    std::vector<char> body(body_size);
    auto p = &body[0];
    for (auto const k : keys) {
      *reinterpret_cast<uint32_t *>(p) = k.size();
      p += sizeof(uint32_t);
      std::copy_n(k.data(), k.size(), p);
      p += k.size();
    }
    co_await send_all(*state->conn, body.data(), body_size);
    pool.put_conn(state);
  }
  co_await std::suspend_always{};
}

task<std::pair<std::vector<char>, std::vector<key_value_view>>>
remote_list(conn_pool &pool, std::unordered_set<std::string_view> const &keys) {
  auto body_size = sizeof(uint32_t);
  for (auto const &key : keys) {
    body_size += sizeof(uint32_t) + key.size();
  }
  auto header_body = std::vector<char>(kRPCRequestHeaderSize);
  auto pending = new pending_call;
  *reinterpret_cast<uint64_t *>(&header_body[0]) =
      reinterpret_cast<uint64_t>(pending);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::list;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto conn = co_await pool.get_conn();
  struct iovec header_iov[2];
  uint32_t count = keys.size();
  header_iov[0].iov_base = &header_body[0];
  header_iov[0].iov_len = kRPCRequestHeaderSize;
  header_iov[1].iov_base = &count;
  header_iov[1].iov_len = sizeof(count);
  co_await writev_all(*conn->conn, header_iov, 2);
  auto t = send_list_keys(conn, pool, keys, body_size - sizeof(count));
  pending->h = t.h_;
  co_await t;
  std::vector<char> response = std::move(pending->response);
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

task<void> remote_zadd(conn_pool &pool, std::string_view key,
                       std::string_view value, uint32_t score) {
  uint32_t key_size_align = align_up<sizeof(uint32_t)>(key.size());
  uint32_t value_size_align = align_up<sizeof(uint32_t)>(value.size());
  auto body_size = key_size_align + value_size_align + sizeof(uint32_t) * 3;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zadd;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key_size_align;
  *reinterpret_cast<uint32_t *>(body_p) = value.size();
  body_p += sizeof(uint32_t);
  std::copy_n(value.data(), value.size(), body_p);
  body_p += value_size_align;
  *reinterpret_cast<uint32_t *>(body_p) = score;
  co_await rpc_call(pool, std::move(header_body),
                    kRPCRequestHeaderSize + body_size);
}

task<std::optional<std::pair<std::vector<char>, std::vector<score_value_view>>>>
remote_zrange(conn_pool &pool, std::string_view key, uint32_t min_score,
              uint32_t max_score) {
  uint32_t key_size_align = align_up<sizeof(uint32_t)>(key.size());
  auto body_size = sizeof(uint32_t) * 3 + key_size_align;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zrange;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key_size_align;
  *reinterpret_cast<uint32_t *>(body_p) = min_score;
  body_p += sizeof(uint32_t);
  *reinterpret_cast<uint32_t *>(body_p) = max_score;
  auto response = co_await rpc_call(pool, std::move(header_body),
                                    kRPCRequestHeaderSize + body_size);
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
  uint32_t key_size_align = align_up<sizeof(uint32_t)>(key.size());
  uint32_t value_size_align = align_up<sizeof(uint32_t)>(value.size());
  auto body_size = key_size_align + value_size_align + sizeof(uint32_t) * 2;
  auto header_body = std::vector<char>(kRPCRequestHeaderSize + body_size);
  *reinterpret_cast<method *>(&header_body[sizeof(uint64_t)]) = method::zrmv;
  *reinterpret_cast<uint32_t *>(&header_body[0] + sizeof(uint64_t) +
                                sizeof(method)) = body_size;
  auto body_p = &header_body[0] + kRPCRequestHeaderSize;
  *reinterpret_cast<uint32_t *>(body_p) = key.size();
  body_p += sizeof(uint32_t);
  std::copy_n(key.data(), key.size(), body_p);
  body_p += key_size_align;
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
    req_id = 0;
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
        co_await listener.accept(loops[rpc_conn_id % loops.size()]);
    {
      int flags = 1;
      setsockopt(conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags,
                 sizeof(flags));
    }
    handle_rpc(std::move(conn), rpc_conn_id++).detach();
  }
  co_return;
}

task<void> send_all(uringpp::socket &conn, const char *data, size_t size) {
  while (size > 0) {
    auto n = co_await conn.send(data, size, MSG_NOSIGNAL);
    if (n <= 0) {
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

task<void> handle_http(uringpp::socket conn, size_t conn_id) {
  auto conn_shard = conn_id % loops.size();
  auto loop = loops[conn_shard];
  co_await loop->switch_to_io_thread(shard_id);
  try {
    io_buffer request(65536);
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
            fmt::print("{} {} recv error: {}\n", shard_id, conn_id,
                       strerror(-n));
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
          if (kv_initialized_) {
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
            auto value = co_await xcore_query(key);
            if (!value.has_value()) {
              co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              co_await send_text(conn, value->data(), value->length());
            }
          } else {
            auto value =
                co_await remote_query(rpc_clients[key_shard][conn_shard], key);
            if (!value.has_value()) {
              co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
            } else {
              co_await send_text(conn, value->second.data(),
                                 value->second.length());
            }
          }
        } break;
        case 'd': {
          // del
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          std::string_view key(path + 5, path_len - 5);
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            co_await xcore_del(key);
          } else {
            co_await remote_del(rpc_clients[key_shard][conn_shard], key);
          }
        } break;
        case 'z': {
          // zrmv
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
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
            co_await xcore_zrmv(key, value);
          } else {
            co_await remote_zrmv(rpc_clients[key_shard][conn_shard], key,
                                 value);
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
              fmt::print("RPC client connected\n");
            }
          }
          {
            bool init = false;
            if (kv_initializing_.compare_exchange_strong(init, true)) {
              auto load_thread = std::thread([]() {
                enable_on_cores(cores);
                init_load();
              });
              load_thread.detach();
            }
          }
          co_await send_all(conn, OK_RESPONSE, sizeof(OK_RESPONSE) - 1);
        } break;
        case 'a': {
          // add
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          simdjson::padded_string_view json = simdjson::padded_string_view(
              body_start, content_length,
              request.capacity() - request.read_idx());
          auto doc = parser.parse(json);
          auto const key = doc["key"].get_string().take_value();
          auto const value = doc["value"].get_string().take_value();
          auto key_shard = get_shard(key);
          if (key_shard == me) {
            co_await xcore_add(key, value);
          } else {
            co_await remote_add(rpc_clients[key_shard][conn_shard], key, value);
          }
        } break;
        case 'b': {
          // batch
          co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
          auto json = simdjson::padded_string_view(body_start, content_length,
                                                   request.capacity() -
                                                       request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::vector<std::vector<key_value_view>> sharded_keys(nr_peers);
          for (auto &&kv : arr) {
            auto const key = kv["key"].get_string().take_value();
            auto const value = kv["value"].get_string().take_value();
            auto key_shard = get_shard(key);
            sharded_keys[key_shard].emplace_back(key, value);
          }
          std::vector<task<void>> tasks;
          for (auto key_shard = 0; key_shard < nr_peers; ++key_shard) {
            if (key_shard == me) {
              continue;
            }
            tasks.emplace_back(remote_batch(rpc_clients[key_shard][conn_shard],
                                            sharded_keys[key_shard]));
          }
          tasks.emplace_back(xcore_batch(sharded_keys[me]));
          for (auto &&task : tasks) {
            co_await task;
          }
        } break;
        case 'l': {
          // list
          auto json = simdjson::padded_string_view(body_start, content_length,
                                                   request.capacity() -
                                                       request.read_idx());
          auto doc = parser.parse(json);
          auto arr = doc.get_array().take_value();
          std::vector<std::unordered_set<std::string_view>> sharded_keys(
              nr_peers);
          std::vector<std::vector<key_value_view>> remote_key_values;
          for (auto &&k : arr) {
            auto key = k.get_string().take_value();
            auto key_shard = get_shard(key);
            sharded_keys[key_shard].insert(key);
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
          auto local_kv_count = 0;
          auto local_key_values = co_await xcore_list(sharded_keys[me].begin(),
                                                      sharded_keys[me].end());
          for (auto &core_kv : local_key_values) {
            local_kv_count += core_kv.size();
          }
          for (auto &t : tasks) {
            auto remote_kvs = co_await t;
            remote_kv_count += remote_kvs.second.size();
            remote_buffers.emplace_back(std::move(remote_kvs.first));
            remote_key_values.emplace_back(std::move(remote_kvs.second));
          }
          if (local_kv_count == 0 && remote_kv_count == 0) {
            co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
          } else {
            co_await send_all(conn, CHUNK_RESPONSE, sizeof(CHUNK_RESPONSE) - 1);
            auto write_kv = [](rapidjson::StringBuffer &buffer,
                               std::string_view key, std::string_view value) {
              auto d = rapidjson::Document();
              d.SetObject();
              auto &a = d.GetAllocator();
              d.AddMember("key", rapidjson::StringRef(key.data(), key.size()),
                          a);
              d.AddMember("value",
                          rapidjson::StringRef(value.data(), value.size()), a);
              rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
              d.Accept(writer);
            };
            constexpr size_t batch_size = 1000;
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
            uint32_t local_kv_written = 0;
            for (auto &core_kv : local_key_values) {
              for (auto &kv : core_kv) {
                auto &buf = buffer[batch_idx];
                write_kv(buffer[batch_idx], kv.key, kv.value);
                ++local_kv_written;
                if (local_kv_written != local_kv_count || remote_kv_count) {
                  buf.Put(',');
                }
                if (++batch_idx == batch_size) {
                  co_await send_kv_chunk();
                }
              }
            }
            uint32_t remote_kv_written = 0;
            for (auto &remote_kv : remote_key_values) {
              for (auto &kv : remote_kv) {
                auto &buf = buffer[batch_idx];
                write_kv(buffer[batch_idx], kv.key, kv.value);
                ++remote_kv_written;
                if (remote_kv_written != remote_kv_count) {
                  buf.Put(',');
                }
                if (++batch_idx == batch_size) {
                  co_await send_kv_chunk();
                }
              }
            }
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
            co_await send_all(conn, EMPTY_RESPONSE, sizeof(EMPTY_RESPONSE) - 1);
            std::string_view key(&path[6], path_len - 6);
            simdjson::padded_string_view json = simdjson::padded_string_view(
                body_start, content_length,
                request.capacity() - request.read_idx());
            auto score_value = parser.parse(json);
            auto value = score_value["value"].get_string().take_value();
            auto score = score_value["score"].get_uint64().take_value();
            auto key_shard = get_shard(key);
            if (key_shard == me) {
              co_await xcore_zadd(key, value, score);
            } else {
              co_await remote_zadd(rpc_clients[key_shard][conn_shard], key,
                                   value, score);
            }
          } break;
          case 'r': {
            // zrange
            std::string_view key(&path[8], path_len - 8);
            simdjson::padded_string_view json = simdjson::padded_string_view(
                body_start, content_length,
                request.capacity() - request.read_idx());
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
            constexpr size_t batch_size = 1000;
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
              auto score_values =
                  co_await xcore_zrange(key, min_score, max_score);
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
              auto score_values =
                  co_await remote_zrange(rpc_clients[key_shard][conn_shard],
                                         key, min_score, max_score);
              if (!score_values.has_value()) {
                co_await send_all(conn, HTTP_404, sizeof(HTTP_404) - 1);
              } else if (score_values->second.empty()) {
                co_await send_all(conn, EMPTY_ARRAY, sizeof(EMPTY_ARRAY) - 1);
              } else {
                co_await send_all(conn, CHUNK_RESPONSE,
                                  sizeof(CHUNK_RESPONSE) - 1);
                uint32_t sv_written = 0;
                for (auto const sv : score_values->second) {
                  auto &buf = buffer[batch_idx];
                  write_sv(buffer[batch_idx], sv.value, sv.score);
                  if (++sv_written != score_values->second.size()) {
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

task<void> db_flusher() {
  auto loop = loops[shard_id];
  rocksdb::FlushOptions flush_options;
  for (;;) {
    struct __kernel_timespec ts = {0, 500000000};
    co_await loop->timeout(&ts);
    kv_db_->Flush(flush_options);
    zset_db_->Flush(flush_options);
  }
}

task<void> connect_rpc_client(std::string port) {
  (void)loop_started.load();
  try {
    co_await main_loop->switch_to_io_thread(shard_id);
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
  } catch (std::exception &e) {
    fmt::print("Failed to connect to peers {}\n", e.what());
    peers_updated = false;
    co_return;
  }
}

static inline rocksdb::Options get_open_options() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.use_adaptive_mutex = true;
  options.unordered_write = true;
  options.enable_write_thread_adaptive_yield = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.DisableExtraChecks();
  return options;
}

static inline rocksdb::ReadOptions get_bulk_read_options() {
  rocksdb::ReadOptions read_options;
  read_options.verify_checksums = false;
  read_options.fill_cache = false;
  read_options.readahead_size = 128 * 1024 * 1024;
  read_options.async_io = true;
  return read_options;
}

void init_load() {
  char *init_dirs_env = std::getenv("INIT_DIRS");
  if (init_dirs_env == nullptr) {
    kv_initialized_ = true;
    return;
  }
  write_options.disableWAL = true;
  std::vector<std::string> init_dirs;
  boost::split(init_dirs, init_dirs_env, boost::is_any_of(","));
  std::vector<std::vector<std::string>> ssts;
  std::mutex sst_m;
  std::chrono::steady_clock::time_point start =
      std::chrono::steady_clock::now();
  std::atomic<size_t> key_count;
  {
    std::vector<std::thread> threads;
    for (auto dir : init_dirs) {
      rocksdb::DB *load_db;
      auto status =
          rocksdb::DB::OpenForReadOnly(get_open_options(), dir, &load_db);
      if (!status.ok()) {
        continue;
      }
      threads.emplace_back([load_db, dir, &sst_m, &ssts, &key_count]() {
        size_t count = 0;
        std::vector<rocksdb::SstFileWriter *> writers;
        std::vector<std::string> sst_paths;
        for (size_t i = 0; i < cores.size(); ++i) {
          writers.push_back(new rocksdb::SstFileWriter(rocksdb::EnvOptions(),
                                                       get_open_options()));
          auto sst_path =
              std::filesystem::path(dir) / fmt::format("load-{}.sst", i);
          auto status = writers[i]->Open(sst_path.string());
          if (!status.ok()) {
            fmt::print("Failed to open SST file {}: {}\n", sst_path.string(),
                       status.ToString());
            return;
          }
          sst_paths.push_back(sst_path.string());
          std::vector<char> zero(1, '\0');
          writers[i]->Put(rocksdb::Slice(zero.data(), zero.size()),
                          rocksdb::Slice(zero.data(), zero.size()));
        }
        auto it = load_db->NewIterator(get_bulk_read_options());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          auto key = it->key();
          auto value = it->value();
          auto key_sv = key.ToStringView();
          auto value_sv = value.ToStringView();
          if (get_shard(key_sv) == me) {
            auto core = get_core(key_sv);
            writers[core]->Put(key_sv, value_sv);
            {
              mutable_pair<std::string, std::string> new_kv{
                  std::string(key_sv), std::string(value_sv)};
              std::lock_guard lk(*init_locks_[core]);
              kv_shards_[core]->insert(std::move(new_kv));
            }
            count++;
          }
        }
        delete it;
        delete load_db;
        for (auto writer : writers) {
          auto status = writer->Finish();
          if (!status.ok()) {
            fmt::print("Failed to finish SST file: {}\n", status.ToString());
          }
          delete writer;
        }
        {
          std::lock_guard lock(sst_m);
          ssts.push_back(sst_paths);
        }
        key_count += count;
      });
    }
    for (auto &t : threads) {
      t.join();
    }
  }
  rocksdb::IngestExternalFileOptions ingest_options;
  ingest_options.move_files = true;
  ingest_options.snapshot_consistency = false;
  ingest_options.allow_global_seqno = true;
  for (size_t core = 0; core < cores.size(); ++core) {
    std::vector<std::string> this_shard_ssts;
    for (auto &sst_paths : ssts) {
      this_shard_ssts.push_back(sst_paths[core]);
    }
    auto status = kv_db_shards_[core]->IngestExternalFile(this_shard_ssts,
                                                          ingest_options);
    if (!status.ok()) {
      fmt::print("Failed to ingest SST files: {}\n", status.ToString());
    }
  }
  kv_initialized_.store(true);
  fmt::print("Loaded {} keys\n", key_count);
}

int main(int argc, char *argv[]) {
  shard_id = 0;
  cores = get_cpu_affinity();
  loops.resize(cores.size());
  kv_shards_.resize(cores.size());
  zset_shards_.resize(cores.size());
  kv_db_shards_.resize(cores.size());
  zset_db_shards_.resize(cores.size());
  init_locks_.resize(cores.size());
  for (size_t i = 0; i < cores.size(); ++i) {
    persist_queue_.emplace_back(new io_queue{});
    persist_efds_.push_back(eventfd(0, 0));
    persist_queue_.back()->set_efd(persist_efds_.back());
    init_locks_[i] = new std::mutex();
    kv_shards_[i] = new unordered_string_map<std::string>();
    zset_shards_[i] = new unordered_string_map<std::unique_ptr<zset_stl>>();
    rocksdb::DB *kv_db;
    rocksdb::DB *zset_db;
    auto kv_db_path = fmt::format("/data/{}/kv", i);
    std::filesystem::create_directories(kv_db_path);
    auto zset_db_path = fmt::format("/data/{}/zset", i);
    std::filesystem::create_directories(zset_db_path);
    auto status = rocksdb::DB::Open(get_open_options(), kv_db_path, &kv_db);
    if (!status.ok()) {
      fmt::print("Failed to open kv db {}\n", status.ToString());
      exit(1);
    }
    kv_db_shards_[i] = kv_db;
    status = rocksdb::DB::Open(get_open_options(), zset_db_path, &zset_db);
    if (!status.ok()) {
      fmt::print("Failed to open zset db {}\n", status.ToString());
      exit(1);
    }
    zset_db_shards_[i] = zset_db;
  }
  main_loop =
      loop_with_queue::create(cores.size(), 16384, IORING_SETUP_SQPOLL, -1, 0);
  main_loop->waker();
  ::signal(SIGPIPE, SIG_IGN);
  bind_cpu(cores[0]);
  for (int i = 0; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i]] {
      shard_id = i;
      kvs_ = kv_shards_[i];
      zsets_ = zset_shards_[i];
      kv_db_ = kv_db_shards_[i];
      zset_db_ = zset_db_shards_[i];
      fmt::print("thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      {
        auto count = 0;
        auto it = std::unique_ptr<rocksdb::Iterator>(
            kv_db_->NewIterator(get_bulk_read_options()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          auto key = it->key();
          auto value = it->value();
          kvs_->insert({key.ToString(), value.ToString()});
          count++;
        }
        fmt::print("shard {} kv count: {}\n", shard_id, count);
      }
      {
        auto it = std::unique_ptr<rocksdb::Iterator>(
            zset_db_->NewIterator(get_bulk_read_options()));
        auto count = 0;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          auto full_key = it->key();
          auto score_raw = it->value();
          auto score = *reinterpret_cast<uint32_t const *>(score_raw.data());
          auto [key, value] = decode_zset_key(full_key);
          xcore_zadd_local_no_persist(key, value, score);
          count++;
        }
        fmt::print("shard {} zset count: {}\n", shard_id, count);
      }
      auto persist_thread = std::thread([i]() {
        for (;;) {
          uint64_t count;
          ::read(persist_efds_[i], &count, sizeof(count));
          while (!persist_queue_[i]->empty()) {
            persist_queue_[i]->consume_one([](auto h) { h.resume(); });
          }
        }
      });
      persist_thread.detach();
      auto loop = loop_with_queue::create(
          cores.size(), 8192, IORING_SETUP_ATTACH_WQ, main_loop->fd());
      loops[i] = loop;
      loop_started.fetch_add(1);
      loop->waker();
      db_flusher().detach();
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
  return 0;
}
