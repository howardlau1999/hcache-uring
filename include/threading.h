#pragma once

#include <vector>
#include <boost/lockfree/policies.hpp>
#include <boost/lockfree/queue.hpp>
#include <coroutine>
#include <memory>
#include <uringpp/event_loop.h>

void bind_cpu(int core);

std::vector<int> get_cpu_affinity();

class io_queue {
  boost::lockfree::queue<std::coroutine_handle<>,
                         boost::lockfree::capacity<128>>
      queue_;

public:
  struct thread_switch_awaitable {
    io_queue &queue_;
    thread_switch_awaitable(io_queue &queue) : queue_(queue) {}
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const noexcept {
      queue_.push(h);
    }
    void await_resume() const noexcept {}
  };

  thread_switch_awaitable queue_in_loop() { return {*this}; }

  void push(std::coroutine_handle<> h) { queue_.push(h); }

  template <class F> void consume_one(F fn) { queue_.consume_one(fn); }

  template <class F> void consume_all(F fn) { queue_.consume_all(fn); }

  bool empty() const { return queue_.empty(); }
};

class loop_with_queue : public uringpp::event_loop {
  io_queue queue_;

public:
  static std::shared_ptr<loop_with_queue>
  create(unsigned int entries = 128, uint32_t flags = 0, int wq_fd = -1);

  loop_with_queue(unsigned int entries = 128, uint32_t flags = 0,
                  int wq_fd = -1);

  template <class T> void block_on(uringpp::task<T> t) {
    while (!t.h_.done()) {
      poll_no_wait();
      run_pending();
    }
  }

  void run_pending() {
    queue_.consume_all([](std::coroutine_handle<> h) { h.resume(); });
  }

  io_queue::thread_switch_awaitable switch_to_io_thread() {
    return queue_.queue_in_loop();
  }
};