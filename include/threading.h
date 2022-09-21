#pragma once

#include <atomic>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <cassert>
#include <coroutine>
#include <memory>
#include <uringpp/event_loop.h>
#include <vector>

extern thread_local size_t shard_id;

void bind_cpu(int core);

std::vector<int> get_cpu_affinity();

void enable_on_cores(std::vector<int> const &cores);

class io_queue {
  boost::lockfree::spsc_queue<std::coroutine_handle<>,
                              boost::lockfree::capacity<256>>
      queue_;
  std::atomic<bool> sleeping_ = true;
  int efd_;

public:
  struct thread_switch_awaitable {
    io_queue &queue_;
    thread_switch_awaitable(io_queue &queue) : queue_(queue) {}
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      queue_.push(h);
      queue_.maybe_wakeup();
    }
    void await_resume() const noexcept {}
  };

  thread_switch_awaitable queue_in_loop() { return {*this}; }

  void push(std::coroutine_handle<> h) { queue_.push(h); }

  template <class F> void consume_one(F &&fn) { queue_.consume_one(fn); }

  void sleep() {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    sleeping_.store(true, std::memory_order_relaxed);
  }

  bool empty() const { return !queue_.read_available(); }

  void maybe_wakeup() {
    std::atomic_signal_fence(std::memory_order_seq_cst);
    if (sleeping_.load(std::memory_order_relaxed)) {
      sleeping_.store(false, std::memory_order_relaxed);
      wakeup(efd_);
    }
  }

  void wakeup(int efd) const {
    uint64_t val = 1;
    auto ret = ::write(efd, &val, sizeof(val));
    assert(ret > 0);
  }

  void set_efd(int efd) { efd_ = efd; }
};

class loop_with_queue : public uringpp::event_loop {
  std::vector<io_queue> queues_;
  std::vector<int> efds_;

public:
  static std::shared_ptr<loop_with_queue>
  create(size_t cores, unsigned int entries = 128, uint32_t flags = 0,
         int wq_fd = -1, int sq_thread_cpu = -1, int sq_thread_idle = 2000);

  loop_with_queue(size_t cores, unsigned int entries = 128, uint32_t flags = 0,
                  int wq_fd = -1, int sq_thread_cpu = -1,
                  int sq_thread_idle = 2000);

  template <class T> void block_on(uringpp::task<T> t) {
    while (!t.h_.done()) {
      poll();
    }
  }

  auto waker_loop(size_t i) -> uringpp::task<void> {
    for (;;) {
      uint64_t val = 0;
      int n = co_await this->read(efds_[i], &val, sizeof(val));
      run_pending(i);
      if (n <= 0) {
        co_return;
      }
    }
  }

  void waker() {
    for (size_t i = 0; i < queues_.size(); ++i) {
      waker_loop(i).detach();
    }
  }

  void run_pending(size_t shard_id) {
    constexpr size_t run_task_limit = 256;
    size_t count = 0;
    auto &q = queues_[shard_id];
    while (!q.empty()) {
      q.consume_one([](std::coroutine_handle<> h) { h.resume(); });
      if (++count == run_task_limit) {
        break;
      }
    }
    if (!q.empty()) {
      q.wakeup(efds_[shard_id]);
    } else {
      q.sleep();
    }
  }

  io_queue::thread_switch_awaitable switch_to_io_thread(size_t from_shard) {
    assert(from_shard < queues_.size());
    return queues_[from_shard].queue_in_loop();
  }
};