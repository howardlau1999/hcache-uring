#include "threading.h"
#include "uringpp/event_loop.h"
#include "uringpp/listener.h"
#include <fmt/format.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <uringpp/uringpp.h>

using uringpp::task;

std::vector<std::shared_ptr<loop_with_queue>> loops;
std::atomic<size_t> loop_started;

task<void> send_worker(std::shared_ptr<loop_with_queue> loop) {
  auto conn = co_await uringpp::socket::connect(loop, "28.10.10.75", "12345");
  {
    int flags = 1;
    setsockopt(conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
  }
  std::vector<char> buf(256 * 1024);
  for (;;) {
    auto n = co_await conn.send(buf.data(), buf.size(), MSG_NOSIGNAL);
    if (n <= 0) {
      fmt::print("send_worker: send failed {}\n", n);
      break;
    }
    n = co_await conn.recv(&buf[0], buf.size(), MSG_NOSIGNAL);
    if (n <= 0) {
      fmt::print("recv_worker: recv failed {}\n", n);
      break;
    }
  }
}

task<void> recv_worker(uringpp::socket conn, size_t conn_id) {
  auto shard = conn_id % loops.size();
  auto loop = loops[shard];
  co_await loop->switch_to_io_thread();
  fmt::print("recv_worker: conn_id {} shard {}\n", conn_id, shard);
  auto buf = std::vector<char>(256 * 1024);
  size_t total = 0;
  for (;;) {
    auto n = co_await conn.recv(&buf[0], buf.size(), MSG_NOSIGNAL);
    if (n <= 0) {
      fmt::print("recv_worker: recv failed {}\n", n);
      break;
    }
    n = co_await conn.send(buf.data(), buf.size(), MSG_NOSIGNAL);
    if (n <= 0) {
      fmt::print("recv_worker: send failed {}\n", n);
      break;
    }
    total += 1;
  }
}

task<void> server(std::shared_ptr<uringpp::event_loop> loop) {
  auto listener = uringpp::listener::listen(loop, "0.0.0.0", "12345");
  size_t conn_id = 0;
  for (;;) {
    auto [addr, conn] =
        co_await listener.accept(loops[conn_id % loops.size()]);
    {
      int flags = 1;
      setsockopt(conn.fd(), SOL_TCP, TCP_NODELAY, (void *)&flags,
                 sizeof(flags));
    }
    fmt::print("accepted connection from {}:{}\n", addr.ip(), addr.port());
    recv_worker(std::move(conn), conn_id).detach();
    conn_id++;
  }
}

int main(int argc, char *argv[]) {
  auto main_loop = uringpp::event_loop::create();
  ::signal(SIGPIPE, SIG_IGN);
  auto cores = get_cpu_affinity();
  loops.resize(cores.size());
  for (int i = 0; i < cores.size(); ++i) {
    auto thread = std::thread([i, core = cores[i], main_loop, argc] {
      fmt::print("thread {} bind to core {}\n", i, core);
      bind_cpu(core);
      auto loop = loop_with_queue::create(1024, IORING_SETUP_ATTACH_WQ,
                                          main_loop->fd());
      if (argc > 1) {
        send_worker(loop).detach();
      }
      loop->waker().detach();
      loops[i] = loop;
      loop_started++;
      for (;;) {
        loop->poll();
      }
    });
    thread.detach();
  }
  while (loop_started < loops.size()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  if (argc == 1) {
    server(main_loop).detach();
  }
  for (;;) {
    main_loop->poll();
  }
}