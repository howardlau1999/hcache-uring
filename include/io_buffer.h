#pragma once

#include <cassert>
#include <cstddef>
#include <list>
#include <vector>

class io_buffer {
  std::vector<char> buffer_;
  size_t read_idx_ = 0;
  size_t write_idx_ = 0;

public:
  io_buffer(size_t size) : buffer_(size) {}

  char *read_data() { return buffer_.data() + read_idx_; }
  char *write_data() { return buffer_.data() + write_idx_; }
  size_t capacity() const { return buffer_.size(); }
  size_t read_idx() const { return read_idx_; }
  size_t write_idx() const { return write_idx_; }
  size_t readable() const { return write_idx_ - read_idx_; }
  size_t writable() const { return buffer_.size() - write_idx_; }
  void reset() {
    read_idx_ = 0;
    write_idx_ = 0;
  }
  void advance_read(size_t n) {
    read_idx_ += n;
    if (read_idx_ == write_idx_)
      reset();
  }
  void advance_write(size_t n) { write_idx_ += n; }
  void expand(size_t n) { buffer_.resize(buffer_.size() + n); }
};

struct output_page {
  char *data = nullptr;
  size_t size = 0;
  size_t sent = 0;
  bool owned = false;
};

class json_output_stream {
public:
  std::list<output_page> pages_;
  output_page *current_page_ = nullptr;
  size_t size_ = 0;
  size_t page_size = 16384;

  typedef char Ch;
  Ch Peek() const {
    assert(false);
    return '\0';
  }
  Ch Take() {
    assert(false);
    return '\0';
  }
  size_t Tell() const {
    assert(false);
    return 0;
  }

  Ch *PutBegin() {
    assert(false);
    return 0;
  }
  void Put(Ch c) {
    if (current_page_ == nullptr || current_page_->size == page_size) {
      page_size += page_size;
      pages_.emplace_back(new char[page_size], 0, 0, true);
      current_page_ = &pages_.back();
    }
    current_page_->data[current_page_->size++] = c;
    size_++;
  }
  void Flush() {}
  size_t GetSize() const { return size_; }
  size_t PutEnd(Ch *) {
    assert(false);
    return 0;
  }
};