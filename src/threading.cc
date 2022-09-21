#include "threading.h"
#include <fmt/format.h>
#include <sys/eventfd.h>

std::shared_ptr<loop_with_queue>
loop_with_queue::create(size_t cores, unsigned int entries, uint32_t flags,
                        int wq_fd, int sq_thread_cpu, int sq_thread_idle) {
  return std::make_shared<loop_with_queue>(cores, entries, flags, wq_fd,
                                           sq_thread_cpu, sq_thread_idle);
}

loop_with_queue::loop_with_queue(size_t cores, unsigned int entries,
                                 uint32_t flags, int wq_fd, int sq_thread_cpu,
                                 int sq_thread_idle)
    : uringpp::event_loop(entries, flags, wq_fd, sq_thread_cpu, sq_thread_idle),
      queues_(cores) {
  for (auto &queue : queues_) {
    int efd = ::eventfd(0, 0);
    if (efd < 0) {
      throw std::runtime_error(
          fmt::format("failed to create eventfd: {}", std::strerror(errno)));
    }
    queue.set_efd(efd);
    efds_.push_back(efd);
  }
}

std::vector<int> get_cpu_affinity() {
  std::vector<int> cores;
  cpu_set_t cpuset;
  if (auto rc =
          ::pthread_getaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
      rc != 0) {
    fmt::print("failed to get affinity {}\n", ::strerror(errno));
    return cores;
  }
  for (int i = 0; i < CPU_SETSIZE; ++i) {
    if (CPU_ISSET(i, &cpuset)) {
      cores.push_back(i);
    }
  }
  return cores;
}

void enable_on_cores(std::vector<int> const &cores) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  for (auto core : cores) {
    CPU_SET(core, &cpuset);
  }
  if (auto rc =
          ::pthread_setaffinity_np(::pthread_self(), sizeof(cpuset), &cpuset);
      rc != 0) {
    fmt::print("failed to set affinity {}\n", ::strerror(errno));
  }
}

void bind_cpu(int core) {
  cpu_set_t cpuset;

  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  if (auto rc =
          ::pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
      rc != 0) {
    return;
  }
}