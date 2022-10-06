#include "storage.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
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
  options.create_if_missing = true;
  options.allow_mmap_reads = true;
  options.allow_mmap_writes = true;
  options.use_adaptive_mutex = true;
  options.unordered_write = true;
  options.enable_write_thread_adaptive_yield = true;
  options.write_buffer_size = 256 * 1024 * 1024;
  options.DisableExtraChecks();
  options.IncreaseParallelism(4);
  return options;
}

storage::storage() : kv_initialized_(false), kv_initializing_(false) {
  write_options_.disableWAL = true;
  rocksdb::DB *db;
  auto status = rocksdb::DB::Open(get_open_options(), zset_dir, &db);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  zset_db_ = std::unique_ptr<rocksdb::DB>(db);
}

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
  read_options.readahead_size = 128 * 1024 * 1024;
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
  std::vector<std::string> ssts;
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
      threads.emplace_back([load_db, dir, &sst_m, &ssts, &key_count, this]() {
        cds::threading::Manager::attachThread();
        size_t count = 0;
        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(),
                                               get_open_options());
        auto sst_path = std::filesystem::path(dir) / "load.sst";
        auto status = sst_file_writer.Open(sst_path.string());
        if (!status.ok()) {
          fmt::print("Failed to open SST file {}: {}\n", sst_path.string(),
                     status.ToString());
          return;
        }
        auto it = load_db->NewIterator(get_bulk_read_options());
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          auto key = it->key();
          auto value = it->value();
          auto key_sv = key.ToStringView();
          auto value_sv = value.ToStringView();
          if (get_shard(key_sv) == me) {
            add_no_persist(key_sv, value_sv);
            sst_file_writer.Put(key_sv, value_sv);
            count++;
          }
        }
        cds::threading::Manager::detachThread();
        status = sst_file_writer.Finish();
        delete it;
        delete load_db;
        if (!status.ok()) {
          fmt::print("Failed to finish SST file {}: {}\n", sst_path.string(),
                     status.ToString());
          return;
        }
        {
          std::lock_guard lock(sst_m);
          ssts.push_back(sst_path);
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
  kv_db_->IngestExternalFile(ssts, ingest_options);
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  fmt::print("KV DB initialized {} keys in {}ms\n", key_count,
             std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
                 .count());
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
    auto [key, value] = decode_zset_key(full_key);
    if (value.size() == 0) {
      auto shard = get_key_shard(key);
      // {
      //   std::lock_guard lock(zsets_mutex_[shard]);
      //   zsets_[shard].insert({std::string(key), std::make_unique<zset_stl>()});
      // }
      
    } else {
      auto score_raw = it->value();
      auto score = *reinterpret_cast<uint32_t const *>(score_raw.data());
      zadd_no_persist(key, value, score);
    }
    count++;
  }
  fmt::print("Loaded {} ZSET keys\n", count);
}

size_t storage::get_key_shard(std::string_view key) const {
  return XXH64(key.data(), key.size(), 19260817) % nr_shards;
}

std::optional<std::string> storage::query(std::string_view key) {
  std::optional<std::string> ret = std::nullopt;
  kvs_.find(key, [&](auto &kv, ...) {
    ret = kv.value;
  });
  // auto shard = get_key_shard(key);
  // {
  //   std::shared_lock lock(kvs_mutex_[shard]);
  //   auto it = kvs_[shard].find(key);
  //   if (it != kvs_[shard].end()) {
  //     ret = it->second;
  //   }
  // }
  return ret;
}

bool storage::add_no_persist(std::string_view key, std::string_view value) {
  if (kvs_.find(key, [value](auto &kv, ...) {
        kv.value.assign(value.begin(), value.end());
      })) {
    return true;
  }
  key_value_intl *kv = new key_value_intl(key, value);
  auto [success, is_new] =
      kvs_.update(*kv, [](bool inserted, auto &old_kv, auto &new_kv) {
        if (!inserted) {
          old_kv.value = new_kv.value;
        }
      });
  if (!is_new) {
    mi_disposer<key_value_intl>()(kv);
  }
  return true;
  // auto shard = get_key_shard(key);
  // {
  //   std::shared_lock lock(zsets_mutex_[shard]);
  //   if (auto it = zsets_[shard].find(key); it != zsets_[shard].end()) {
  //     return false;
  //   }
  // }
  // {
  //   std::unique_lock lock(kvs_mutex_[shard]);
  //   kvs_[shard].insert({std::string(key), std::string(value)});
  //   return true;
  // }
}

rocksdb::WriteBatch *storage::start_batch() {
  return new rocksdb::WriteBatch();
}
void storage::commit_batch(rocksdb::WriteBatch *batch) {
  // kv_db_->Write(write_options_, batch);
  delete batch;
}

void storage::add_batch(rocksdb::WriteBatch *batch, std::string_view key,
                        std::string_view value) {
  add_no_persist(key, value);
  // batch->Put(key, value);
}

bool storage::add(std::string_view key, std::string_view value) {
  if (!add_no_persist(key, value)) {
    return false;
  }
  // kv_db_->Put(write_options_, key, value);
  return true;
}

void storage::del(std::string_view key) {
  bool remove_kv = false;
  bool remove_zset = false;
  if (auto p = kvs_.erase(key); p) {
    mi_disposer<key_value_intl>()(p);
    remove_kv = true;
  }

  if (auto p = zsets_.erase(key); p) {
    mi_disposer<zset_intl>()(p);
    remove_zset = true;
  }
  auto shard = get_key_shard(key); 
  // {
  //   std::unique_lock lock(kvs_mutex_[shard]);
  //   auto it = kvs_[shard].find(key);
  //   if (it != kvs_[shard].end()) {
  //     kvs_[shard].erase(it);
  //     remove_kv = true;
  //   }
  // }
  // {
  //   std::unique_lock lock(zsets_mutex_[shard]);
  //   auto it = zsets_[shard].find(key);
  //   if (it != zsets_[shard].end()) {
  //     zsets_[shard].erase(it);
  //     remove_zset = true;
  //   }
  // }
  if (remove_kv) {
    // kv_db_->Delete(write_options_, key);
  }
  if (remove_zset) {
    // std::vector<char> prefix(key.begin(), key.end());
    // prefix.push_back(0);
    // auto prefix_sv = std::string_view(prefix.data(), prefix.size());
    // auto it = std::unique_ptr<rocksdb::Iterator>(
    //     zset_db_->NewIterator(get_bulk_read_options()));
    // it->Seek(rocksdb::Slice(prefix.data(), prefix.size()));
    // while (it->Valid()) {
    //   auto full_key = it->key();
    //   if (!full_key.ToStringView().starts_with(prefix_sv)) {
    //     break;
    //   }
    //   zset_db_->Delete(write_options_, full_key);
    //   it->Next();
    // }
  }
}

void zset_stl::zadd(uint32_t score, std::string_view value) {
  std::lock_guard lock(mutex);
  if (auto it = value_score.find(value); it != value_score.end()) {
    auto old_score = it->second;
    if (old_score != score) {
      auto &old_values = score_values[old_score];
      old_values.erase(old_values.find(value));
      score_values[score].insert(index_string{std::string(value)});
      it->second = score;
    }
  } else {
    value_score.insert({std::string(value), score});
    score_values[score].insert(index_string{std::string(value)});
  }
}

bool storage::zadd_no_persist(std::string_view key, std::string_view value,
                              uint32_t score) {
  if (zsets_.find(key, [value, score](zset_intl &zset, ...) {
        zset.zadd(score, value);
      })) {
    return true;
  }
  zset_intl *zset = new zset_intl(key);
  auto [success, is_new] =
      zsets_.update(*zset, [value, score](bool inserted, zset_intl &old_zset,
                                          zset_intl &new_zset) {
        if (!inserted) {
          old_zset.zadd(score, value);

        } else {
          new_zset.zadd(score, value);
        }
      });
  if (!is_new) {
    mi_disposer<zset_intl>()(zset);
  }
  return true;
  // auto shard = get_key_shard(key);
  // bool is_new = false;
  // {
  //   std::unique_lock lock(zsets_mutex_[shard]);
  //   auto it = zsets_[shard].find(key);
  //   if (it != zsets_[shard].end()) {
  //     it->second->zadd(score, value);
  //   } else {
  //     auto zset = std::make_unique<zset_stl>();
  //     zset->zadd(score, value);
  //     zsets_[shard].insert({std::string(key), std::move(zset)});
  //     is_new = true;
  //   }
  // }
  // return is_new;
}

bool storage::zadd(std::string_view key, std::string_view value,
                   uint32_t score) {
  // {
  //   auto shard = get_key_shard(key);
  //   std::shared_lock lock(kvs_mutex_[shard]);
  //   auto it = kvs_[shard].find(key);
  //   if (it != kvs_[shard].end()) {
  //     return false;
  //   }
  // }
  bool is_new = zadd_no_persist(key, value, score);
  // auto full_key = encode_zset_key(key, value);
  // zset_db_->Put(
  //     write_options_, rocksdb::Slice(full_key.data(), full_key.size()),
  //     rocksdb::Slice(reinterpret_cast<const char *>(&score), sizeof(score)));
  return true;
}

void storage::zrmv(std::string_view key, std::string_view value) {
  auto full_key = encode_zset_key(key, value);
  // zset_db_->Delete(write_options_,
  //                  rocksdb::Slice(full_key.data(), full_key.size()));
  zsets_.find(key, [key, value](zset_intl &zset, ...) {
    std::lock_guard lock(zset.mutex);
    if (auto it = zset.value_score.find(value); it != zset.value_score.end())
    {
      auto score = it->second;
      zset.value_score.erase(it);
      auto &values = zset.score_values[score];
      values.erase(values.find(value));
      if (values.empty()) {
        zset.score_values.erase(score);
      }
      if (zset.value_score.empty()) {
        zset.value_score.clear();
      }
    }
  });
  // auto shard = get_key_shard(key);
  // {
  //   std::unique_lock lock(zsets_mutex_[shard]);
  //   auto it = zsets_[shard].find(key);
  //   if (it != zsets_[shard].end()) {
  //     auto score_it = it->second->value_score.find(value);
  //     if (score_it != it->second->value_score.end()) {
  //       auto score = score_it->second;
  //       it->second->value_score.erase(score_it);
  //       auto &values = it->second->score_values[score];
  //       values.erase(values.find(value));
  //     }
  //   }
  // }
}

std::optional<std::vector<score_value>>
storage::zrange(std::string_view key, uint32_t min_score, uint32_t max_score) {
  std::vector<score_value> ret;
  if (!zsets_.find(key, [min_score, max_score, &ret](zset_intl &zset, ...) {
        std::lock_guard lock(zset.mutex);
        for (auto it = zset.score_values.lower_bound(min_score);
             it != zset.score_values.end() && it->first <= max_score; it++) {
          for (auto &&value : it->second) {
            ret.emplace_back(score_value{value.key, it->first});
          }
        }
      })) {
    return std::nullopt;
  }
  // auto shard = get_key_shard(key);
  // {
  //   std::shared_lock lock(zsets_mutex_[shard]);
  //   auto zset_it = zsets_[shard].find(key);
  //   if (zset_it != zsets_[shard].end()) {
  //     for (auto it = zset_it->second->score_values.lower_bound(min_score);
  //          it != zset_it->second->score_values.end() && it->first <= max_score;
  //          it++) {
  //       for (auto &&value : it->second) {
  //         ret.emplace_back(score_value{value.key, it->first});
  //       }
  //     }
  //   } else {
  //     return std::nullopt;
  //   }
  // }
  return ret;
}
