#pragma once

#include <cds/intrusive/options.h>
#include <cds/opt/compare.h>
#include <cds/opt/options.h>
#include <rocksdb/slice.h>
#include <algorithm>
#include <cds/intrusive/cuckoo_set.h>
#include <cds/intrusive/michael_list_hp.h>
#include <cds/intrusive/michael_set.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <rocksdb/db.h>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include "rpc.h"
#include "uringpp/task.h"


using uringpp::task;

struct key_value_intl
    : public cds::intrusive::cuckoo::node<cds::intrusive::cuckoo::list, 2> {
  std::string key;
  std::string value;
  key_value_intl(std::string_view k, std::string_view v)
      : key(k.data(), k.size()), value(v.data(), v.size()) {}
};

struct zset_intl
    : public cds::intrusive::cuckoo::node<cds::intrusive::cuckoo::list, 2> {
  std::string key;
  zset_intl(std::string_view k) : key(k) {}
  std::map<uint32_t, std::unordered_set<std::string>> score_values;
  std::unordered_map<std::string, uint32_t> value_score;
  std::shared_mutex mutex;
  void zadd(uint32_t score, std::string_view value);
};

template <typename T> struct mi_disposer {
  void operator()(void *p) {
    auto ptr = reinterpret_cast<T *>(p);
    delete ptr;
  }
};

template <class KeyValue> struct key_value_compare {
  int operator()(const KeyValue &lhs, const KeyValue &rhs) const {
    if (lhs.key < rhs.key) {
      return -1;
    } else if (lhs.key == rhs.key) {
      return 0;
    } else {
      return 1;
    }
  }

  int operator()(const KeyValue &lhs, const std::string_view &rhs) const {
    if (lhs.key < rhs) {
      return -1;
    } else if (lhs.key == rhs) {
      return 0;
    } else {
      return 1;
    }
  }

  int operator()(const std::string_view &lhs, const KeyValue &rhs) const {
    if (lhs < rhs.key) {
      return -1;
    } else if (lhs == rhs.key) {
      return 0;
    } else {
      return 1;
    }
  }
};

struct hash1 {
  size_t operator()(const zset_intl &zset) const { return (*this)(zset.key); }
  size_t operator()(const key_value_intl &kv) const { return (*this)(kv.key); }
  size_t operator()(std::string_view key) const {
    return cds::opt::v::hash<std::string_view>{}(key);
  }
  size_t operator()(const std::string &key) const {
    return cds::opt::v::hash<std::string>{}(key);
  }
};

struct hash2 : public hash1 {
  size_t operator()(const zset_intl &zset) const { return (*this)(zset.key); }
  size_t operator()(const key_value_intl &kv) const { return (*this)(kv.key); }
  size_t operator()(std::string_view key) const {
    size_t h1 = ~(hash1::operator()(key));
    return h1 * 19260817 + (h1 << 5) + (h1 >> 2);
  }
  size_t operator()(const std::string &key) const {
    return (*this)(std::string_view(key));
  }
};

template <class T>
struct cuckoo_set
    : public cds::intrusive::CuckooSet<
          T, typename cds::intrusive::cuckoo::make_traits<
                 cds::intrusive::opt::hook<cds::intrusive::cuckoo::base_hook<
                     cds::intrusive::cuckoo::probeset_type<
                         typename T::probeset_type>,
                     cds::intrusive::cuckoo::store_hash<T::hash_array_size>>>,
                 cds::opt::hash<std::tuple<hash1, hash2>>,
                 cds::intrusive::opt::disposer<mi_disposer<T>>,
                 cds::opt::compare<key_value_compare<T>>>::type> {};

typedef cuckoo_set<key_value_intl> kv_cuckoo_set;

typedef cuckoo_set<zset_intl> zset_cuckoo_set;

class storage {
  std::vector<char> peers_;
  std::unique_ptr<rocksdb::DB> kv_db_;
  std::unique_ptr<rocksdb::DB> zset_db_;
  std::atomic<uint32_t> me_;
  std::atomic<bool> kv_initialized_;

  kv_cuckoo_set kvs_;
  zset_cuckoo_set zsets_;

  void open_kv_db();

public:
  storage();

  void flush();
  void first_time_init();
  void load_kv();
  void load_zset();
  bool kv_loaded() const { return kv_initialized_; }

  task<void> start_rpc_server();

  std::vector<key_view_value>
  list(std::unordered_set<std::string_view>::iterator begin,
       std::unordered_set<std::string_view>::iterator end) {
    std::vector<key_view_value> ret;
    for (auto it = begin; it != end; ++it) {
      if (auto v = query(*it); v.has_value()) {
        ret.emplace_back(*it, std::move(v.value()));
      }
    }
    return ret;
  }

  std::vector<key_value>
  list(std::unordered_set<std::string>::iterator begin,
       std::unordered_set<std::string>::iterator end) {
    std::vector<key_value> ret;
    for (auto it = begin; it != end; ++it) {
      if (auto v = query(*it); v.has_value()) {
        ret.emplace_back(std::move(*it), std::move(v.value()));
      }
    }
    return ret;
  }

  uint32_t me() const { return me_; }
  size_t get_shard(std::string_view key) const {
    return std::hash<std::string_view>{}(key) % peers_.size();
  }
  size_t nr_peer() const { return peers_.size(); }

  task<void> try_update_peer();

  std::optional<std::string> query(std::string_view key);
  task<std::optional<std::string>>
  remote_query(size_t shard, std::string_view key);

  task<std::vector<key_value>>
  remote_list(size_t shard, std::unordered_set<std::string_view> &&keys);

  task<void> remote_batch(size_t shared,
                                     std::vector<key_value_view> &&kvs);

  void add_no_persist(std::string_view key, std::string_view value);
  void add(std::string_view key, std::string_view value);
  task<void> remote_add(size_t shard, std::string_view key,
                                   std::string_view value);

  void del(std::string_view key);
  task<void> remote_del(size_t shard, std::string_view key);

  void zadd_no_persist(std::string_view key, std::string_view value,
                       uint32_t score);
  void zadd(std::string_view key, std::string_view value, uint32_t score);
  task<void> remote_zadd(size_t shard, std::string_view key,
                                    std::string_view value, uint32_t score);

  void zrmv(std::string_view key, std::string_view value);
  task<void> remote_zrmv(size_t shard, std::string_view key,
                                    std::string_view value);

  std::optional<std::vector<score_value>>
  zrange(std::string_view key, uint32_t min_score, uint32_t max_score);
  task<std::optional<std::vector<score_value>>>
  remote_zrange(size_t shard, std::string_view key, uint32_t min_score,
                uint32_t max_score);

  static std::vector<char> encode_zset_key(std::string_view key,
                                           std::string_view value) {
    std::vector<char> full_key(key.size() + value.size() + 1);
    std::copy(key.begin(), key.end(), full_key.begin());
    full_key[key.size()] = '\0';
    std::copy(value.begin(), value.end(), full_key.begin() + key.size() + 1);
    return full_key;
  }

  static std::pair<std::string_view, std::string_view>
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

  std::atomic<bool> peers_updated_;
  std::atomic<bool> kv_initializing_;
};