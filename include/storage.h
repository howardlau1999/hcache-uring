#pragma once

#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "rpc.h"
#include "uringpp/task.h"
#include "xxhash.h"
#include <algorithm>
#include <ankerl/unordered_dense.h>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index_container.hpp>
#include <cds/intrusive/cuckoo_set.h>
#include <cds/intrusive/michael_list_hp.h>
#include <cds/intrusive/michael_set.h>
#include <cds/intrusive/options.h>
#include <cds/opt/compare.h>
#include <cds/opt/options.h>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace boost::multi_index;

struct string_hash {
  using is_transparent = void; // enable heterogenous lookup
  using is_avalanching = void; // mark class as high quality avalanching hash

  [[nodiscard]] auto operator()(const char *str) const noexcept -> uint64_t {
    return ankerl::unordered_dense::hash<std::string_view>{}(str);
  }

  [[nodiscard]] auto operator()(std::string_view str) const noexcept
      -> uint64_t {
    return ankerl::unordered_dense::hash<std::string_view>{}(str);
  }

  [[nodiscard]] auto operator()(std::string const &str) const noexcept
      -> uint64_t {
    return ankerl::unordered_dense::hash<std::string_view>{}(str);
  }
};

using ankerlkv = ankerl::unordered_dense::map<std::string, std::string,
                                              string_hash, std::equal_to<>>;
using ankerlkvview =
    ankerl::unordered_dense::map<std::string_view, std::string_view,
                                 string_hash, std::equal_to<>>;
using ankerlkvset = ankerl::unordered_dense::set<std::string_view, string_hash,
                                                 std::equal_to<>>;

template <typename T, typename Q> struct mutable_pair {
  T first;
  mutable Q second;
};

struct index_string {
  std::string key;
};

struct string_view_hash {
  std::size_t operator()(const std::string_view &v) const {
    return XXH64(v.data(), v.size(), 19260817);
  }
  std::size_t operator()(const std::string &s) const {
    return XXH64(s.data(), s.size(), 19260817);
  }
};

struct string_view_equal_to {
  std::size_t operator()(const std::string &s1, const std::string &s2) const {
    return s1 == s2;
  }
  std::size_t operator()(const std::string &s1,
                         const std::string_view &v2) const {
    return s1.size() == v2.size() &&
           std::equal(s1.begin(), s1.end(), v2.begin());
  }
  std::size_t operator()(const std::string_view &v1,
                         const std::string &s2) const {
    return v1.size() == s2.size() &&
           std::equal(v1.begin(), v1.end(), s2.begin());
  }
};

template <typename Q>
using unordered_string_map = multi_index_container<
    mutable_pair<std::string, Q>,
    indexed_by<hashed_unique<member<mutable_pair<std::string, Q>, std::string,
                                    &mutable_pair<std::string, Q>::first>,
                             string_view_hash, string_view_equal_to>>>;

using unordered_string_set = multi_index_container<
    index_string, indexed_by<hashed_unique<
                      member<index_string, std::string, &index_string::key>,
                      string_view_hash, string_view_equal_to>>>;

size_t get_shard(std::string_view key);
extern size_t nr_peers;
extern size_t me;

struct key_value_intl
    : public cds::intrusive::cuckoo::node<cds::intrusive::cuckoo::list, 2> {
  std::string key;
  std::string value;
  key_value_intl(std::string_view k, std::string_view v)
      : key(k.data(), k.size()), value(v.data(), v.size()) {}
};

struct zset_stl {
  std::map<uint32_t, unordered_string_set> score_values;
  unordered_string_map<uint32_t> value_score;
  std::shared_mutex mutex;
  void zadd(uint32_t score, std::string_view value);
};

using ankerlzset = ankerl::unordered_dense::map<std::string, zset_stl *,
                                                string_hash, std::equal_to<>>;

struct zset_intl
    : public cds::intrusive::cuckoo::node<cds::intrusive::cuckoo::list, 2>,
      public zset_stl {
  std::string key;
  zset_intl(std::string_view k) : key(k) {}
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
    return XXH64(key.data(), key.size(), 19260817);
  }
  size_t operator()(const std::string &key) const {
    return XXH64(key.data(), key.size(), 19260817);
  }
};

struct hash2 : public hash1 {
  size_t operator()(const zset_intl &zset) const { return (*this)(zset.key); }
  size_t operator()(const key_value_intl &kv) const { return (*this)(kv.key); }
  size_t operator()(std::string_view key) const {
    return std::hash<std::string_view>()(key);
  }
  size_t operator()(const std::string &key) const {
    return std::hash<std::string_view>()(key);
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
  static constexpr size_t nr_shards = 64;
  std::unique_ptr<rocksdb::DB> kv_db_;
  std::unique_ptr<rocksdb::DB> zset_db_;
  std::atomic<bool> kv_initialized_;
  rocksdb::WriteOptions write_options_;

  ankerlkv kvs_[nr_shards];
  std::shared_mutex kvs_mutex_[nr_shards];
  ankerlzset zsets_[nr_shards];
  std::shared_mutex zsets_mutex_[nr_shards];
  // kv_cuckoo_set kvs_;
  // zset_cuckoo_set zsets_;

  void open_kv_db();
  size_t get_key_shard(std::string_view key) const;

public:
  storage();

  void flush();
  void first_time_init();
  void load_kv();
  void load_zset();
  bool kv_loaded() const { return kv_initialized_; }

  std::vector<key_view_value> list(ankerlkvset::iterator begin,
                                   ankerlkvset::iterator end) {
    std::vector<key_view_value> ret;
    for (auto it = begin; it != end; ++it) {
      if (auto v = query(*it); v.has_value()) {
        ret.emplace_back(*it, v.value());
      }
    }
    return ret;
  }

  std::optional<std::string> query(std::string_view key);

  rocksdb::WriteBatch *start_batch();
  void commit_batch(rocksdb::WriteBatch *batch);
  void add_batch(rocksdb::WriteBatch *batch, std::string_view key,
                 std::string_view value);
  bool add_no_persist(std::string_view key, std::string_view value);
  bool add(std::string_view key, std::string_view value);
  void del(std::string_view key);
  bool zadd_no_persist(std::string_view key, std::string_view value,
                       uint32_t score);
  bool zadd(std::string_view key, std::string_view value, uint32_t score);

  void zrmv(std::string_view key, std::string_view value);

  std::optional<std::vector<score_value>>
  zrange(std::string_view key, uint32_t min_score, uint32_t max_score);

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