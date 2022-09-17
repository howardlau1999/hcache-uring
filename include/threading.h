#pragma once

#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <coroutine>
#include <memory>
#include <uringpp/event_loop.h>
#include <vector>

void bind_cpu(int core);

std::vector<int> get_cpu_affinity();

void enable_on_cores(std::vector<int> const &cores);

class io_queue {
  boost::lockfree::queue<std::coroutine_handle<>,
                         boost::lockfree::capacity<8192>>
      queue_;

public:
  struct thread_switch_awaitable {
    io_queue &queue_;
    int efd_;
    thread_switch_awaitable(io_queue &queue, int efd)
        : queue_(queue), efd_(efd) {}
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      queue_.push(h);
      uint64_t val = 1;
      (void)::write(efd_, &val, sizeof(val));
    }
    void await_resume() const noexcept {}
  };

  thread_switch_awaitable queue_in_loop(int efd) { return {*this, efd}; }

  void push(std::coroutine_handle<> h) { queue_.push(h); }

  template <class F> void consume_one(F fn) { queue_.consume_one(fn); }

  template <class F> void consume_all(F fn) { queue_.consume_all(fn); }

  bool empty() const { return queue_.empty(); }
};

class loop_with_queue : public uringpp::event_loop {
  io_queue queue_;
  int efd_ = -1;

public:
  static std::shared_ptr<loop_with_queue>
  create(unsigned int entries = 128, uint32_t flags = 0, int wq_fd = -1,
         int sq_thread_cpu = -1, int sq_thread_idle = 2000);

  loop_with_queue(unsigned int entries = 128, uint32_t flags = 0,
                  int wq_fd = -1, int sq_thread_cpu = -1,
                  int sq_thread_idle = 2000);

  template <class T> void block_on(uringpp::task<T> t) {
    while (!t.h_.done()) {
      poll();
    }
  }

  uringpp::task<void> waker() {
    for (;;) {
      uint64_t val = 0;
      int n = co_await this->read(efd_, &val, sizeof(val));
      run_pending();
      if (n <= 0) {
        co_return;
      }
    }
  }

  void run_pending() {
    queue_.consume_all([](std::coroutine_handle<> h) { h.resume(); });
  }

  io_queue::thread_switch_awaitable switch_to_io_thread() {
    return queue_.queue_in_loop(efd_);
  }
};