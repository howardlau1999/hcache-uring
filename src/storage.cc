#include "storage.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_writer.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cds/init.h>
#include <cstdint>
#include <filesystem>
#include <fmt/format.h>
#include <fstream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <vector>

char const *kv_dir = "/data/kv";
char const *zset_dir = "/data/zset";

static inline rocksdb::Options get_open_options() {
  rocksdb::Options options;
  options.write_buffer_size = 512 * 1024 * 1024;
  options.create_if_missing = true;
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.use_adaptive_mutex = true;
  options.unordered_write = true;
  options.DisableExtraChecks();
  options.IncreaseParallelism(4);
  return options;
}

storage::storage() : kv_initialized_(false), kv_initializing_(false) {

  rocksdb::DB *db;
  auto status = rocksdb::DB::Open(get_open_options(), zset_dir, &db);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  zset_db_ = std::unique_ptr<rocksdb::DB>(db);
}

task<void> storage::start_rpc_server() { co_return; }

void storage::open_kv_db() {
  fmt::print("Opening KV DB {}\n", kv_dir);
  rocksdb::DB *db;
  auto status = rocksdb::DB::Open(get_open_options(), kv_dir, &db);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  kv_db_ = std::unique_ptr<rocksdb::DB>(db);
}

static inline rocksdb::ReadOptions get_bulk_read_options() {
  rocksdb::ReadOptions read_options;
  read_options.verify_checksums = false;
  read_options.fill_cache = false;
  read_options.readahead_size = 512 * 1024 * 1024;
  read_options.async_io = true;
  return read_options;
}

void storage::first_time_init() {
  char *init_dirs_env = std::getenv("INIT_DIRS");
  if (init_dirs_env == nullptr) {
    kv_initialized_ = true;
    return;
  }
  std::vector<std::string> init_dirs;
  boost::split(init_dirs, init_dirs_env, boost::is_any_of(","));
  std::vector<std::filesystem::path> ssts;
  std::mutex sst_m;
  {
    std::vector<std::jthread> threads;
    for (auto dir : init_dirs) {
      rocksdb::DB *load_db;
      auto status =
          rocksdb::DB::OpenForReadOnly(get_open_options(), dir, &load_db);
      if (!status.ok()) {
        continue;
      }
      threads.emplace_back([load_db, dir, &sst_m, &ssts, this]() {
        auto it = load_db->NewIterator(get_bulk_read_options());
        auto write_options = rocksdb::WriteOptions();
        write_options.disableWAL = true;
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          auto key = it->key();
          auto value = it->value();
          auto key_sv = key.ToStringView();
          auto value_sv = value.ToStringView();
          if (get_shard(key_sv) == me) {
            add_no_persist(key_sv, value_sv);
            kv_db_->Put(write_options, key_sv, value_sv);
          }
        }
        delete it;
        delete load_db;
      });
    }
  }
  fmt::print("Bulk load finished\n");
  kv_initialized_ = true;
}

void storage::load_kv() {
  open_kv_db();
  size_t count = 0;
  fmt::print("Start loading (not first time)\n");
  auto it = std::unique_ptr<rocksdb::Iterator>(
      kv_db_->NewIterator(get_bulk_read_options()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto key = it->key();
    auto value = it->value();
    add_no_persist(key.ToStringView(), value.ToStringView());
    count++;
  }
  fmt::print("Loaded {} keys from db\n", count);
}

void storage::flush() {
  if (kv_db_) {
    kv_db_->Flush(rocksdb::FlushOptions());
  }
  if (zset_db_) {
    zset_db_->Flush(rocksdb::FlushOptions());
  }
}

void storage::load_zset() {
  fmt::print("Loading ZSET\n");
  auto it = std::unique_ptr<rocksdb::Iterator>(
      zset_db_->NewIterator(get_bulk_read_options()));
  auto count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    auto full_key = it->key();
    auto score_raw = it->value();
    auto score = *reinterpret_cast<uint32_t const *>(score_raw.data());
    auto [key, value] = decode_zset_key(full_key);
    zadd_no_persist(key, value, score);
    count++;
  }
  fmt::print("Loaded {} ZSET keys\n", count);
}

task<void> storage::try_update_peer() {
  // if (!co_await seastar::file_exists("/data/cluster.json")) {
  //   co_return;
  // }
  // bool updated = false;
  // if (peers_updated_.compare_exchange_strong(updated, true)) {
  //   fmt::print("Updating peer info");
  //   auto f = co_await seastar::open_file_dma("/data/cluster.json",
  //                                            seastar::open_flags::ro);
  //   auto size = co_await f.size();
  //   auto stream = seastar::make_file_input_stream(std::move(f));
  //   auto buf = co_await stream.read_exactly(size);
  //   co_await stream.close();
  //   simdjson::dom::parser parser;
  //   simdjson::padded_string_view json =
  //       simdjson::padded_string_view(buf.begin(), buf.size());
  //   auto document = parser.parse(json);
  //   auto hosts = document["hosts"].get_array().take_value();
  //   auto index = document["index"].get_uint64().take_value();
  //   co_await update_peers(std::move(hosts), index);
  // }
  co_return;
}

std::pair<char *, size_t> storage::query(std::string_view key) {
  size_t ret_size = 0;
  char *ret_value = nullptr;
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  // kvs_.find(key, [&](auto &kv, ...) {
  //   ret_value = new char[kv.value.size()];
  //   ret_size = kv.value.size();
  //   std::copy_n(kv.value.data(), kv.value.size(), ret_value);
  // });
  auto shard = std::hash<std::string_view>()(key) % nr_shards;
  {
    std::shared_lock lock(kvs_mutex_[shard]);
    auto it = kvs_[shard].find(key);
    if (it != kvs_[shard].end()) {
      ret_value = new char[it->second.size()];
      ret_size = it->second.size();
      std::copy_n(it->second.data(), it->second.size(), ret_value);
    }
  }
  return {ret_value, ret_size};
}

void storage::add_no_persist(std::string_view key, std::string_view value) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  // if (kvs_.find(key,
  //               [value](auto &kv, ...) { kv.value = std::string(value); })) {
  //   return;
  // }
  // key_value_intl *kv = new key_value_intl(key, value);
  // kvs_.update(*kv, [](bool inserted, auto &old_kv, auto &new_kv) {
  //   if (!inserted) {
  //     old_kv.value = new_kv.value;
  //     mi_disposer<key_value_intl>()(&new_kv);
  //   }
  // });
  {
    auto shard = std::hash<std::string_view>()(key) % nr_shards;
    std::unique_lock lock(kvs_mutex_[shard]);
    kvs_[shard].insert({std::string(key), std::string(value)});
  }
}

static inline rocksdb::WriteOptions get_write_options() {
  rocksdb::WriteOptions write_options;
  write_options.disableWAL = true;
  return write_options;
}

void storage::add(std::string_view key, std::string_view value) {
  add_no_persist(key, value);
  kv_db_->Put(get_write_options(), key, value);
}

void storage::del(std::string_view key) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  auto shard = std::hash<std::string_view>()(key) % nr_shards;
  {
    std::unique_lock lock(kvs_mutex_[shard]);
    auto it = kvs_[shard].find(key);
    if (it != kvs_[shard].end()) {
      kvs_[shard].erase(it);
    }
  }
  {
    std::unique_lock lock(zsets_mutex_[shard]);
    auto it = zsets_[shard].find(key);
    if (it != zsets_[shard].end()) {
      zsets_[shard].erase(it);
    }
  }
  kv_db_->Delete(get_write_options(), key);
}

void zset_stl::zadd(uint32_t score, std::string_view value) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  std::lock_guard lock(mutex);
  if (auto it = value_score.find(value); it != value_score.end()) {
    auto old_score = it->second;
    if (old_score != score) {
      auto &old_values = score_values[old_score];
      old_values.erase(old_values.find(value));
      score_values[score].emplace(std::string(value));
      it->second = score;
    }
  } else {
    value_score.insert({std::string(value), score});
    score_values[score].emplace(std::string(value));
  }
}

void storage::zadd_no_persist(std::string_view key, std::string_view value,
                              uint32_t score) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  // if (zsets_.find(key, [value, score](zset_intl &zset, ...) {
  //       zset.zadd(score, value);
  //     })) {
  //   return;
  // }
  // zset_intl *zset = new zset_intl(key);
  // zsets_.update(*zset, [value, score](bool inserted, zset_intl &old_zset,
  //                                     zset_intl &new_zset) {
  //   if (!inserted) {
  //     old_zset.zadd(score, value);
  //     mi_disposer<zset_intl>()(&new_zset);
  //   } else {
  //     new_zset.zadd(score, value);
  //   }
  // });
  auto shard = std::hash<std::string_view>()(key) % nr_shards;
  {
    std::unique_lock lock(zsets_mutex_[shard]);
    auto it = zsets_[shard].find(key);
    if (it != zsets_[shard].end()) {
      it->second->zadd(score, value);
    } else {
      auto zset = std::make_unique<zset_stl>();
      zset->zadd(score, value);
      zsets_[shard].insert({std::string(key), std::move(zset)});
    }
  }
}

void storage::zadd(std::string_view key, std::string_view value,
                   uint32_t score) {
  zadd_no_persist(key, value, score);
  auto full_key = encode_zset_key(key, value);
  zset_db_->Put(
      get_write_options(), rocksdb::Slice(full_key.data(), full_key.size()),
      rocksdb::Slice(reinterpret_cast<const char *>(&score), sizeof(score)));
}

void storage::zrmv(std::string_view key, std::string_view value) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  auto full_key = encode_zset_key(key, value);
  zset_db_->Delete(get_write_options(),
                   rocksdb::Slice(full_key.data(), full_key.size()));
  // zsets_.find(key, [value](zset_intl &zset, ...) {
  //   std::lock_guard lock(zset.mutex);
  //   if (auto it = zset.value_score.find(value);
  //       it != zset.value_score.end()) {
  //     auto score = it->second;
  //     zset.value_score.erase(it);
  //   }
  // });
  auto shard = std::hash<std::string_view>()(key) % nr_shards;
  {
    std::unique_lock lock(zsets_mutex_[shard]);
    auto it = zsets_[shard].find(key);
    if (it != zsets_[shard].end()) {
      auto score_it = it->second->value_score.find(value);
      if (score_it != it->second->value_score.end()) {
        auto score = score_it->second;
        it->second->value_score.erase(score_it);
        auto &values = it->second->score_values[score];
        values.erase(values.find(value));
      }
    }
  }
}

std::optional<std::vector<score_value>>
storage::zrange(std::string_view key, uint32_t min_score, uint32_t max_score) {
  // if (!cds::threading::Manager::isThreadAttached()) {
  //   cds::threading::Manager::attachThread();
  // }
  std::vector<score_value> ret;
  // if (!zsets_.find(key, [min_score, max_score, &ret](zset_intl &zset, ...) {
  //       std::lock_guard lock(zset.mutex);
  //       for (auto it = zset.score_values.lower_bound(min_score);
  //            it != zset.score_values.end() && it->first <= max_score; it++) {
  //         for (auto &&value : it->second) {
  //           ret.emplace_back(score_value{value, it->first});
  //         }
  //       }
  //     })) {
  //   return std::nullopt;
  // }
  auto shard = std::hash<std::string_view>()(key) % nr_shards;
  {
    std::shared_lock lock(zsets_mutex_[shard]);
    auto zset_it = zsets_[shard].find(key);
    if (zset_it != zsets_[shard].end()) {
      for (auto it = zset_it->second->score_values.lower_bound(min_score);
           it != zset_it->second->score_values.end() && it->first <= max_score;
           it++) {
        for (auto &&value : it->second) {
          ret.emplace_back(score_value{value.key, it->first});
        }
      }
    } else {
      return std::nullopt;
    }
  }
  return ret;
}
