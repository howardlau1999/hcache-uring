#pragma once

#include <string>

enum method : uint8_t {
  add,
  query,
  list,
  del,
  batch,
  zadd,
  zrange,
  zrmv,
  cadd,
  cdel,
  cbatch,
};

struct key_value {
  std::string key;
  std::string value;
  key_value(std::string_view const &key, std::string_view const &value)
      : key(key), value(value) {}
  key_value() = default;
  key_value(key_value const &) = default;
  key_value(std::string const &key, std::string const &value)
      : key(key), value(value) {}
  key_value(std::string &&key, std::string &&value)
      : key(std::move(key)), value(std::move(value)) {}
};

struct key_view_value {
  std::string_view key;
  std::string value;
  key_view_value() = default;
  key_view_value(key_view_value &&) = default;
  key_view_value(std::string_view key, std::string const &value)
      : key(key), value(value) {}
  key_view_value(std::string_view key, std::string &&value)
      : key(key), value(std::move(value)) {}
};

struct key_value_view {
  std::string_view key;
  std::string_view value;
  key_value_view(std::string_view const &key, std::string_view const &value)
      : key(key), value(value) {}
  key_value_view() = default;
  key_value_view(key_value_view const &) = default;
  key_value_view(key_value const &kv) : key(kv.key), value(kv.value) {}
};

struct score_value {
  std::string value;
  uint32_t score;
};

struct score_value_view {
  std::string_view value;
  uint32_t score;
  score_value_view(std::string_view value, uint32_t score)
      : value(value), score(score) {}
};