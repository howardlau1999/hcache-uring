#include "threading.h"
#include <fmt/format.h>

std::shared_ptr<loop_with_queue>
loop_with_queue::create(unsigned int entries, uint32_t flags, int wq_fd) {
  return std::make_shared<loop_with_queue>(entries, flags, wq_fd);
}

loop_with_queue::loop_with_queue(unsigned int entries, uint32_t flags,
                                 int wq_fd)
    : uringpp::event_loop(entries, flags, wq_fd), queue_() {}


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