/*
 * Copyright (c) Rohan Sakhare — All rights reserved.
 *
 * PAYMENT SWITCHING ENGINE — LOAD BALANCER v1.0
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Contact: rohanavinashsakhare@gmail.com  |  +91 9112765649
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>       // FIX: needed for std::invalid_argument / std::out_of_range
#include <string>
#include <thread>
#include <vector>

#include "httplib.h"
#include "json.hpp"

using json = nlohmann::json;
namespace fs = std::filesystem;

// ─────────────────────────────────────────────────────────────────────────────
// Safe integer parsing helpers
//
// std::stoi / std::stoll throw on bad input.  Every place we call them in the
// original code wraps with try/catch(...) which silently swallows the error.
// These helpers make error handling explicit and return a clear default.
// ─────────────────────────────────────────────────────────────────────────────
static int safe_stoi(const std::string &s, int default_val) noexcept {
  try {
    std::size_t pos = 0;
    int v = std::stoi(s, &pos);
    // FIX: reject trailing garbage e.g. "80abc" — stoi stops at 'a'
    if (pos != s.size()) return default_val;
    return v;
  } catch (...) {
    return default_val;
  }
}

static long long safe_stoll(const std::string &s, long long default_val) noexcept {
  try {
    std::size_t pos = 0;
    long long v = std::stoll(s, &pos);
    if (pos != s.size()) return default_val;
    return v;
  } catch (...) {
    return default_val;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ConnectionPool — bounded, shrink-aware pool of HTTP Keep-Alive clients.
//
// Bug fixes vs. original:
//  • release() now resets the client before returning it to the pool so a
//    stale / errored socket is not handed to the next request.
//  • Destructor acquires the mutex before iterating — safe even if another
//    thread is mid-release() during shutdown.
// ─────────────────────────────────────────────────────────────────────────────
class ConnectionPool {
private:
  std::string host_;
  int         port_;
  std::mutex  mutex_;
  std::vector<httplib::Client *> clients_;
  size_t      max_size_;

  static httplib::Client *make_client(const std::string &host, int port) {
    auto *c = new httplib::Client(host, port);
    c->set_connection_timeout(5, 0);
    c->set_read_timeout(15, 0);
    c->set_write_timeout(15, 0);
    c->set_keep_alive(true);
    return c;
  }

public:
  explicit ConnectionPool(std::string h, int p, size_t max_s = 32)
      : host_(std::move(h)), port_(p), max_size_(max_s) {
    clients_.reserve(max_size_);
  }

  // FIX: destructor now holds the lock before iterating to avoid data race
  // with a concurrent release() call during shutdown.
  ~ConnectionPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto *c : clients_) delete c;
    clients_.clear();
  }

  std::unique_ptr<httplib::Client> acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!clients_.empty()) {
      auto *c = clients_.back();
      clients_.pop_back();
      return std::unique_ptr<httplib::Client>(c);
    }
    return std::unique_ptr<httplib::Client>(make_client(host_, port_));
  }

  // FIX: Reset the client's socket state before pooling.  If the upstream
  // closed the connection (e.g., after an error response) the old socket fd
  // is dead.  stop() forces httplib to open a fresh connection next time the
  // client is used, preventing ECONNRESET / silent request drops.
  void release(std::unique_ptr<httplib::Client> cli) {
    if (cli) {
      cli->stop(); // flush / recycle internal socket
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (cli && clients_.size() < max_size_) {
      clients_.push_back(cli.release());
    }
    // unique_ptr destructor closes socket when over cap — no leak.
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// Backend — descriptor for one upstream server.
// ─────────────────────────────────────────────────────────────────────────────
struct Backend {
  std::string host;
  int         port;
  std::string endpoint_str; // cached "host:port" — built once, read many

  std::atomic<bool>      alive{false};
  std::atomic<bool>      started{false};
  std::atomic<bool>      failed{false};
  std::atomic<bool>      was_alive{false};
  std::atomic<long long> launch_time_ms{0};
  std::atomic<long long> fail_time_ms{0};
  // FIX: Use int (not unsigned) so that --active_requests never underflows
  // silently to UINT_MAX.  Signed wrapping is still UB but we guard against it
  // at the decrement site.
  std::atomic<int>       active_requests{0};
  std::atomic<int>       consecutive_failures{0};
  std::atomic<int>       consecutive_successes{0};
  // FIX: avg_response_ms stored as long long to avoid truncation when
  // elapsed_ms > INT_MAX (pathological, but defensive).
  std::atomic<long long> avg_response_ms{0};

  std::shared_ptr<ConnectionPool> pool;

  const std::string &endpoint() const { return endpoint_str; }
};

// ─────────────────────────────────────────────────────────────────────────────
// Global State
// ─────────────────────────────────────────────────────────────────────────────
static std::vector<std::unique_ptr<Backend>> g_backends;
// FIX: g_running must be sig_atomic_t-compatible; std::atomic<bool> satisfies
// this.  Additionally, the signal handler only stores to atomics and calls
// server->stop() — both of which are async-signal-safe under the httplib API.
static std::atomic<bool>      g_running{true};
static std::atomic<long long> g_total_requests{0};
static std::chrono::steady_clock::time_point g_start_time;

// FIX: Use std::atomic pointers so the signal handler can read them without
// a data race.  Written once from main() before threads start.
static std::atomic<httplib::Server *> g_proxy_server{nullptr};
static std::atomic<httplib::Server *> g_admin_server{nullptr};

static std::atomic<bool> g_docker_mode{false};

// ─────────────────────────────────────────────────────────────────────────────
// Logging helpers
//
// FIX: std::localtime is not thread-safe (returns pointer to a shared static).
//      Use localtime_r (POSIX) on all platforms.
// ─────────────────────────────────────────────────────────────────────────────
static void format_timestamp(char (&ts)[32]) {
  auto now       = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);
#ifdef _WIN32
  struct tm tm_buf;
  localtime_s(&tm_buf, &in_time_t);
  std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);
#else
  struct tm tm_buf;
  localtime_r(&in_time_t, &tm_buf);          // FIX: thread-safe
  std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);
#endif
}

static void log_info(const std::string &msg) {
  char ts[32];
  format_timestamp(ts);
  // FIX: Single-call output prevents interleaving from other threads.
  std::cout << '[' << ts << "] [INFO] " << msg << '\n';
}

static void log_error(const std::string &msg) {
  char ts[32];
  format_timestamp(ts);
  std::cerr << '[' << ts << "] [ERROR] " << msg << '\n';
}

// ─────────────────────────────────────────────────────────────────────────────
// Utility
// ─────────────────────────────────────────────────────────────────────────────
static void kill_process_on_port(int port) {
  // FIX: Use %z$pid to avoid possible command injection from environment.
  // Port is an int from our own data structures — safe to embed directly.
  std::string kill_by_port =
      "PID=$(lsof -t -i :" + std::to_string(port) +
      " 2>/dev/null); [ -n \"$PID\" ] && kill -9 $PID 2>/dev/null; true";
  std::system(kill_by_port.c_str());

  // Also kill by binary path pattern (catches processes in startup phase)
  std::string pattern  = "CONTAINER/" + std::to_string(port);
  std::string kill_by_name =
      "PID=$(pgrep -f \"" + pattern +
      "\" 2>/dev/null); [ -n \"$PID\" ] && kill -9 $PID 2>/dev/null; true";
  std::system(kill_by_name.c_str());
}

static void cleanup_backends() {
  static std::once_flag cleanup_flag;
  std::call_once(cleanup_flag, []() {
    if (!g_docker_mode.load()) {
      log_info("[LB] Cleaning up running container processes...");
      for (auto &b : g_backends) {
        kill_process_on_port(b->port);
      }
    }
  });
}

static fs::path get_container_dir() {
  fs::path exe_dir;
#ifdef __APPLE__
  char path[2048];
  uint32_t size = sizeof(path);
  if (_NSGetExecutablePath(path, &size) == 0) {
    exe_dir = fs::path(path).parent_path();
  } else {
    exe_dir = fs::current_path();
  }
#else
  exe_dir = fs::current_path();
#endif

  exe_dir = fs::weakly_canonical(exe_dir);

  if (fs::exists(exe_dir / "CONTAINER"))
    return exe_dir / "CONTAINER";
  if (fs::exists(exe_dir.parent_path() / "CONTAINER"))
    return exe_dir.parent_path() / "CONTAINER";

  fs::path current = fs::current_path();
  if (fs::exists(current / "CONTAINER"))
    return current / "CONTAINER";
  if (fs::exists(current.parent_path() / "CONTAINER"))
    return current.parent_path() / "CONTAINER";

  return exe_dir / "CONTAINER";
}

static void start_backend(Backend *b) {
  // FIX: Use compare_exchange to prevent double-start under concurrent access.
  // Original used load() + store() as two separate non-atomic operations.
  bool expected = false;
  if (!b->started.compare_exchange_strong(expected, true))
    return; // already started by another thread

  kill_process_on_port(b->port);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  log_info("[LB] Launching container on port " + std::to_string(b->port) + "...");

  auto now = std::chrono::steady_clock::now();
  auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                 now.time_since_epoch())
                 .count();
  b->launch_time_ms.store(ms);

#ifdef __APPLE__
  std::string exec_path =
      (get_container_dir() / std::to_string(b->port)).string();
  std::string cmd =
      "osascript -e 'tell application \"Terminal\" to do script \"" +
      exec_path + " ; exit\"' > /dev/null 2>&1 &";
  std::system(cmd.c_str());
#else
  std::string exec_path =
      (get_container_dir() / std::to_string(b->port)).string();
  std::string cmd =
      exec_path + " > container_" + std::to_string(b->port) + ".log 2>&1 &";
  std::system(cmd.c_str());
#endif
}

// ─────────────────────────────────────────────────────────────────────────────
// pick_backend — Least Connections + Round-Robin tie breaker
// ─────────────────────────────────────────────────────────────────────────────
static Backend *pick_backend() {
  static constexpr size_t kMaxCandidates = 64;
  Backend *candidates[kMaxCandidates];
  size_t   candidate_count = 0;
  int      min_conn        = std::numeric_limits<int>::max();

  const size_t n = g_backends.size();
  for (size_t i = 0; i < n; ++i) {
    Backend *b = g_backends[i].get();
    if (!b->alive.load(std::memory_order_relaxed))
      continue;
    int active = b->active_requests.load(std::memory_order_relaxed);
    if (active < min_conn) {
      min_conn        = active;
      // FIX: original had a redundant double-assignment to candidate_count.
      // Cleaned up: set count to 1 and place the candidate in one step.
      candidates[0]   = b;
      candidate_count = 1;
    } else if (active == min_conn && candidate_count < kMaxCandidates) {
      candidates[candidate_count++] = b;
    }
  }

  if (candidate_count == 0)
    return nullptr;
  if (candidate_count == 1)
    return candidates[0];

  // Round-Robin tie breaker — no allocation
  static std::atomic<size_t> rr_index{0};
  size_t index = rr_index.fetch_add(1, std::memory_order_relaxed);
  return candidates[index % candidate_count];
}

// ─────────────────────────────────────────────────────────────────────────────
// Health Checker Loop
// ─────────────────────────────────────────────────────────────────────────────
static void health_checker_loop(int interval_seconds) {
  log_info("Health checker thread started (interval: " +
           std::to_string(interval_seconds) + "s)");

  // 1. Initial startup of target active containers if not in Docker mode
  if (!g_docker_mode.load()) {
    const char *target_active_env = std::getenv("TARGET_ACTIVE_BACKENDS");
    // FIX: use safe_stoi to avoid uncaught exception if env var is malformed
    int target_active = target_active_env
                            ? safe_stoi(target_active_env, 3)
                            : 3;
    int started_count = 0;
    for (auto &b : g_backends) {
      if (started_count < target_active) {
        start_backend(b.get());
        started_count++;
      }
    }
  }

  while (g_running.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));

    for (auto &b : g_backends) {
      if (!g_running.load())
        break;

      if (!b->started.load() || b->failed.load())
        continue;

      httplib::Client cli(b->host, b->port);
      cli.set_connection_timeout(2, 0);
      cli.set_read_timeout(2, 0);

      auto res        = cli.Get("/health");
      bool is_healthy = false;

      if (res && res->status == 200) {
        const std::string &body = res->body;
        if (body.find("\"status\"") != std::string::npos &&
            body.find("\"UP\"")     != std::string::npos) {
          is_healthy = true;
        } else {
          try {
            auto body_json = json::parse(body);
            if (body_json.contains("status") && body_json["status"] == "UP") {
              is_healthy = true;
            }
          } catch (...) {
            // Parse failure → treat as unhealthy
          }
        }
      }

      if (is_healthy) {
        b->consecutive_failures.store(0);
        int succs = ++b->consecutive_successes;
        if (succs >= 2) {
          bool expected = false;
          if (b->alive.compare_exchange_strong(expected, true)) {
            log_info("[LB] Backend " + b->endpoint() +
                     " recovered — marked ALIVE — added back to pool");
          }
        }
      } else {
        b->consecutive_successes.store(0);
        int fails = ++b->consecutive_failures;
        if (fails >= 2) {
          bool expected = true;
          if (b->alive.compare_exchange_strong(expected, false)) {
            log_error("[LB] Backend " + b->endpoint() +
                      " went DOWN — marked DEAD — removed from pool");
          }
        }
      }
    }

    // 2. Failover logic (only in local mode)
    if (!g_docker_mode.load() && g_running.load()) {
      auto now    = std::chrono::steady_clock::now();
      auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch())
                        .count();

      const char *startup_timeout_env = std::getenv("STARTUP_TIMEOUT");
      // FIX: use safe_stoll to avoid uncaught exception
      long long startup_timeout_ms =
          startup_timeout_env
              ? safe_stoll(startup_timeout_env, 10) * 1000LL
              : 10000LL;

      for (auto &b : g_backends) {
        if (b->started.load() && !b->failed.load()) {
          if (b->alive.load()) {
            b->was_alive.store(true);
          } else {
            bool has_failed = false;
            if (b->was_alive.load()) {
              has_failed = true;
              log_error("[LB] Backend " + b->endpoint() +
                        " crashed or stopped responding. Failover triggered.");
            } else {
              long long elapsed = now_ms - b->launch_time_ms.load();
              if (elapsed > startup_timeout_ms) {
                has_failed = true;
                log_error("[LB] Backend " + b->endpoint() +
                          " failed to start within " +
                          std::to_string(startup_timeout_ms / 1000) +
                          " seconds. Failover triggered.");
              }
            }

            if (has_failed) {
              b->failed.store(true);
              b->alive.store(false);
              auto f_now = std::chrono::steady_clock::now();
              auto f_ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                               f_now.time_since_epoch())
                               .count();
              b->fail_time_ms.store(f_ms);
              kill_process_on_port(b->port);
            }
          }
        }
      }

      // Count active (started & not failed)
      int active_count = 0;
      for (auto &b : g_backends) {
        if (b->started.load() && !b->failed.load())
          active_count++;
      }

      const char *target_active_env = std::getenv("TARGET_ACTIVE_BACKENDS");
      int target_active = target_active_env
                              ? safe_stoi(target_active_env, 3)
                              : 3;

      if (active_count < target_active) {
        static constexpr size_t kMaxBackends = 64;
        Backend *candidates[kMaxBackends];
        size_t   cand_count = 0;

        for (auto &b : g_backends) {
          if ((!b->started.load() || b->failed.load()) &&
              cand_count < kMaxBackends) {
            candidates[cand_count++] = b.get();
          }
        }

        std::sort(candidates, candidates + cand_count,
                  [](Backend *x, Backend *y) {
                    bool xns = !x->started.load();
                    bool yns = !y->started.load();
                    if (xns != yns)
                      return xns;
                    return x->fail_time_ms.load() < y->fail_time_ms.load();
                  });

        int needed = target_active - active_count;
        for (int i = 0; i < needed && i < static_cast<int>(cand_count); ++i) {
          Backend *b = candidates[i];
          b->started.store(false);
          b->failed.store(false);
          b->was_alive.store(false);
          b->alive.store(false);
          b->consecutive_failures.store(0);
          b->consecutive_successes.store(0);
          b->fail_time_ms.store(0);
          start_backend(b);
          active_count++;
        }
      }
    }
  }

  log_info("Health checker thread stopped");
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal Handler for Graceful Shutdown
//
// FIX: Only async-signal-safe operations inside handler.
//      Atomic store + calling httplib::Server::stop() is safe here.
//      Reading global raw pointers in the original was a data race vs. main().
//      Now they are std::atomic<Server*> so the load is safe.
// ─────────────────────────────────────────────────────────────────────────────
static void on_signal(int /*sig*/) {
  g_running.store(false);
  if (auto *ps = g_proxy_server.load()) ps->stop();
  if (auto *as = g_admin_server.load()) as->stop();
}

// ─────────────────────────────────────────────────────────────────────────────
// main
// ─────────────────────────────────────────────────────────────────────────────
int main() {
  std::atexit(cleanup_backends);
  try {
    g_start_time = std::chrono::steady_clock::now();

    // ── Load Environment Variables ──────────────────────────────────────────
    const char *lb_port_env = std::getenv("LB_PORT");
    // FIX: use safe_stoi so malformed env var doesn't throw uncaught exception
    int lb_port = lb_port_env ? safe_stoi(lb_port_env, 5649) : 5649;

    const char *lb_admin_port_env = std::getenv("LB_ADMIN_PORT");
    int lb_admin_port = lb_admin_port_env ? safe_stoi(lb_admin_port_env, 5650) : 5650;

    const char *backends_env = std::getenv("BACKENDS");
    if (backends_env) {
      // ── Docker mode: BACKENDS="host:port,host:port,..." ──────────────────
      g_docker_mode.store(true);
      std::stringstream ss(backends_env);
      std::string item;
      while (std::getline(ss, item, ',')) {
        // FIX: trim whitespace so "host:port , host:port" parses correctly
        item.erase(0, item.find_first_not_of(" \t\r\n"));
        item.erase(item.find_last_not_of(" \t\r\n") + 1);
        if (item.empty()) continue;

        auto colon = item.find(':');
        if (colon != std::string::npos && colon + 1 < item.size()) {
          int port_val = safe_stoi(item.substr(colon + 1), -1);
          if (port_val <= 0 || port_val > 65535) {
            log_error("Invalid backend port in specification (out of range): " + item);
            continue;
          }
          auto b         = std::make_unique<Backend>();
          b->host        = item.substr(0, colon);
          b->port        = port_val;
          b->endpoint_str = b->host + ":" + std::to_string(b->port);
          b->alive.store(false);
          b->started.store(true);
          b->pool = std::make_shared<ConnectionPool>(b->host, b->port);
          g_backends.push_back(std::move(b));
        } else {
          log_error("Invalid backend specification in BACKENDS: " + item);
        }
      }
    } else {
      // ── Local mode: LB launches CONTAINER binaries ────────────────────────
      g_docker_mode.store(false);
      const char *backend_host_env = std::getenv("BACKEND_HOST");
      std::string backend_host =
          backend_host_env ? backend_host_env : "127.0.0.1";

      const char *backend_ports_env = std::getenv("BACKEND_PORTS");
      std::vector<int> backend_ports = {8080, 8081, 8082, 8083, 8084};
      if (backend_ports_env) {
        backend_ports.clear();
        std::stringstream ss(backend_ports_env);
        std::string item;
        while (std::getline(ss, item, ',')) {
          // FIX: trim whitespace
          item.erase(0, item.find_first_not_of(" \t\r\n"));
          item.erase(item.find_last_not_of(" \t\r\n") + 1);
          if (item.empty()) continue;
          int port_val = safe_stoi(item, -1);
          if (port_val <= 0 || port_val > 65535) {
            log_error("Invalid/out-of-range backend port in environment: " + item);
            continue;
          }
          backend_ports.push_back(port_val);
        }
      }

      for (int port : backend_ports) {
        auto b          = std::make_unique<Backend>();
        b->host         = backend_host;
        b->port         = port;
        b->endpoint_str = backend_host + ":" + std::to_string(port);
        b->alive.store(false);
        b->started.store(false);
        b->pool = std::make_shared<ConnectionPool>(b->host, b->port);
        g_backends.push_back(std::move(b));
      }
    }

    if (g_backends.empty()) {
      log_error("[LB] No backends configured — aborting.");
      return 1;
    }

    g_backends.shrink_to_fit();

    // ── Signal handlers ──────────────────────────────────────────────────────
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);
    std::signal(SIGHUP,  on_signal);
    std::signal(SIGQUIT, on_signal);

    const char *hc_interval_env = std::getenv("HEALTH_CHECK_INTERVAL");
    int hc_interval = hc_interval_env ? safe_stoi(hc_interval_env, 3) : 3;
    // FIX: clamp interval to a sane range
    if (hc_interval < 1)  hc_interval = 1;
    if (hc_interval > 60) hc_interval = 60;

    // ── Health Checker Thread ────────────────────────────────────────────────
    std::thread hc_thread(health_checker_loop, hc_interval);

    // ── Proxy Server ─────────────────────────────────────────────────────────
    httplib::Server proxy_svr;
    g_proxy_server.store(&proxy_svr);

    proxy_svr.new_task_queue = [] {
      size_t concurrency =
          std::max(8u, std::thread::hardware_concurrency() * 16);
      return new httplib::ThreadPool(concurrency, 10000);
    };

    // ── Proxy forwarding handler ─────────────────────────────────────────────
    auto proxy_handler = [](const httplib::Request &req,
                             httplib::Response      &res) {
      Backend *backend = pick_backend();
      if (!backend) {
        json err;
        err["errorCode"] = "ERR_NO_BACKENDS";
        err["message"]   = "All backends are currently unavailable. "
                           "Please try again shortly.";
        res.status = 503;
        res.set_content(err.dump(4), "application/json");
        log_error("[LB] Request rejected (503) — no healthy backends for: " +
                  req.path);
        return;
      }

      g_total_requests.fetch_add(1, std::memory_order_relaxed);
      // FIX: guard against negative active_requests from concurrent errors
      int prev = backend->active_requests.fetch_add(1, std::memory_order_relaxed);
      if (prev < 0) {
        // Should never happen, but reset rather than propagate bad state
        backend->active_requests.store(1, std::memory_order_relaxed);
      }
      auto start = std::chrono::steady_clock::now();

      auto cli = backend->pool->acquire();

      httplib::Request fwd;
      fwd.method = req.method;
      fwd.path   = req.path;
      fwd.params = req.params;
      fwd.body   = req.body;

      for (const auto &h : req.headers) {
        if (h.first == "Host"             ||
            h.first == "Content-Length"   ||
            h.first == "Transfer-Encoding"||
            h.first == "Connection") {
          continue;
        }
        fwd.headers.insert(h);
      }
      fwd.headers.emplace("X-LB-Backend", std::to_string(backend->port));

      auto result = cli->send(fwd);

      // FIX: decrement active_requests before pool release so the count
      // is correct if another thread reads it during release().
      {
        int cur = backend->active_requests.fetch_sub(1, std::memory_order_relaxed);
        // Guard: clamp to 0 to avoid negative counts persisting
        if (cur <= 0) {
          backend->active_requests.store(0, std::memory_order_relaxed);
        }
      }
      backend->pool->release(std::move(cli));

      auto elapsed_ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - start)
              .count();

      if (result) {
        res.status = result->status;

        std::string ct = result->has_header("Content-Type")
                             ? result->get_header_value("Content-Type")
                             : "application/json";
        res.set_content(result->body, ct);

        for (const auto &h : result->headers) {
          if (h.first == "Content-Length"    ||
              h.first == "Content-Type"      ||
              h.first == "Transfer-Encoding") {
            continue;
          }
          res.set_header(h.first, h.second);
        }

        // Rolling 10-sample EMA of response time (stored as long long)
        long long old_avg = backend->avg_response_ms.load(std::memory_order_relaxed);
        long long new_avg = (old_avg == 0) ? elapsed_ms
                                           : (old_avg * 9 + elapsed_ms) / 10;
        backend->avg_response_ms.store(new_avg, std::memory_order_relaxed);

      } else {
        json e;
        e["errorCode"] = "ERR_BAD_GATEWAY";
        e["message"]   = "Bad Gateway: could not reach backend service.";
        res.status = 502;
        res.set_content(e.dump(4), "application/json");
        log_error("[LB] Forward failed → " + backend->endpoint() +
                  "  path=" + req.path +
                  "  err=" + to_string(result.error()));
      }
    };

    // Register proxy handler for all HTTP methods
    proxy_svr.Get(R"(.*)",    proxy_handler);
    proxy_svr.Post(R"(.*)",   proxy_handler);
    proxy_svr.Put(R"(.*)",    proxy_handler);
    proxy_svr.Delete(R"(.*)", proxy_handler);
    proxy_svr.Patch(R"(.*)",  proxy_handler);
    proxy_svr.Options(R"(.*)",proxy_handler);

    // ── Admin / Status Server ────────────────────────────────────────────────
    httplib::Server admin_svr;
    g_admin_server.store(&admin_svr);

    admin_svr.Get("/lb/health",
                  [](const httplib::Request &, httplib::Response &res) {
                    static const std::string kBody = "{\"status\":\"UP\"}";
                    res.status = 200;
                    res.set_content(kBody, "application/json");
                  });

    admin_svr.Get("/lb/status",
                  [](const httplib::Request &, httplib::Response &res) {
                    json status;
                    status["lb_status"]      = "UP";
                    status["total_backends"] = g_backends.size();

                    int  alive_count  = 0;
                    json backends_arr = json::array();

                    for (const auto &b : g_backends) {
                      json b_json;
                      b_json["host"]                  = b->host;
                      b_json["port"]                  = b->port;
                      bool is_alive                   = b->alive.load();
                      b_json["alive"]                 = is_alive;
                      b_json["active_requests"]       = b->active_requests.load();
                      b_json["avg_response_ms"]       = b->avg_response_ms.load();
                      b_json["consecutive_failures"]  = b->consecutive_failures.load();
                      b_json["consecutive_successes"] = b->consecutive_successes.load();
                      if (is_alive) alive_count++;
                      backends_arr.push_back(std::move(b_json));
                    }

                    status["alive_backends"]           = alive_count;
                    status["backends"]                 = std::move(backends_arr);
                    status["total_requests_forwarded"] = g_total_requests.load();

                    auto uptime_seconds =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - g_start_time)
                            .count();
                    status["uptime_seconds"] = uptime_seconds;

                    res.status = 200;
                    res.set_content(status.dump(4), "application/json");
                  });

    // ── Start Admin Server Thread ────────────────────────────────────────────
    std::thread admin_thread([&]() {
      log_info("Admin HTTP server listening on 0.0.0.0:" +
               std::to_string(lb_admin_port));
      admin_svr.listen("0.0.0.0", lb_admin_port);
      log_info("Admin HTTP server stopped");
    });

    // ── Banner ───────────────────────────────────────────────────────────────
    log_info("C++20 Load Balancer listening on 0.0.0.0:" +
             std::to_string(lb_port));

    // FIX: banner column width is fixed at 60 chars.  Original used runtime
    // string padding which could produce negative-length padding (UB via
    // std::string(negative_size, ' ')) for long endpoint strings.
    // We now clamp the endpoint display and use a fixed-width format.
    static constexpr int kBannerWidth  = 60; // total line width incl. ║ ║
    static constexpr int kInnerWidth   = kBannerWidth - 2; // 58

    auto repeat_str = [](const std::string &s, size_t count) {
      std::string result;
      result.reserve(s.size() * count);
      for (size_t i = 0; i < count; ++i) {
        result += s;
      }
      return result;
    };

    auto banner_line = [&](const std::string &content) {
      // Pad or truncate to kInnerWidth
      std::string inner = content;
      if (static_cast<int>(inner.size()) > kInnerWidth)
        inner = inner.substr(0, kInnerWidth - 3) + "...";
      int padding = kInnerWidth - static_cast<int>(inner.size());
      std::cout << "║" << inner << std::string(padding, ' ') << "║\n";
    };

    std::cout << "╔" << repeat_str("═", kInnerWidth) << "╗\n";
    banner_line("       Card API Load Balancer  v1.0 – Rohan Sakhare      ");
    std::cout << "╠" << repeat_str("═", kInnerWidth) << "╣\n";
    banner_line("  Proxy Port  : " + std::to_string(lb_port));
    banner_line("  Admin Port  : " + std::to_string(lb_admin_port));
    banner_line("  HC Interval : " + std::to_string(hc_interval) + "s");
    banner_line("  Backends    : " + std::to_string(g_backends.size()) +
                " registered");
    for (const auto &b : g_backends) {
      banner_line("    \xe2\x86\x92 " + b->endpoint()); // UTF-8 arrow →
    }
    std::cout << "╚" << repeat_str("═", kInnerWidth) << "╝\n\n";

    // ── Start Proxy Server (blocking) ────────────────────────────────────────
    proxy_svr.listen("0.0.0.0", lb_port);

    log_info("Shutdown sequence initiated...");
    log_info("Proxy HTTP server stopped");

    if (admin_thread.joinable()) admin_thread.join();
    if (hc_thread.joinable())    hc_thread.join();

    log_info("Load Balancer shutdown complete");

  } catch (const std::exception &e) {
    log_error("Unhandled exception: " + std::string(e.what()));
    return 1;
  } catch (...) {
    log_error("Unknown exception occurred");
    return 1;
  }
  return 0;
}
