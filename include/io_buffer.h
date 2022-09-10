#pragma once

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