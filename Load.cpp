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
#include <string>
#include <thread>
#include <vector>

#include "httplib.h"
#include "json.hpp"

using json = nlohmann::json;
namespace fs = std::filesystem;

// ── Struct definition for backend registry
struct Backend {
  std::string host;
  int port;
  std::atomic<bool> alive{false};
  std::atomic<bool> started{false};
  std::atomic<bool> failed{false};
  std::atomic<bool> was_alive{false};
  std::atomic<long long> launch_time_ms{0};
  std::atomic<long long> fail_time_ms{0};
  std::atomic<int> active_requests{0};
  std::atomic<int> consecutive_failures{0};
  std::atomic<int> consecutive_successes{0};
  std::atomic<int> avg_response_ms{0};

  // Helper to get formatted string
  std::string endpoint() const { return host + ":" + std::to_string(port); }
};

// ── Global State
static std::vector<std::unique_ptr<Backend>> g_backends;
static std::atomic<bool> g_running{true};
static std::atomic<long long> g_total_requests{0};
static std::chrono::steady_clock::time_point g_start_time;

static httplib::Server *g_proxy_server = nullptr;
static httplib::Server *g_admin_server = nullptr;

static std::atomic<bool> g_docker_mode{false};

// Helper to log with formatted timestamps
static void log_info(const std::string &msg) {
  auto now = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);
  std::cout << "["
            << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S")
            << "] [INFO] " << msg << std::endl;
}

static void log_error(const std::string &msg) {
  auto now = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);
  std::cerr << "["
            << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S")
            << "] [ERROR] " << msg << std::endl;
}

static void kill_process_on_port(int port) {
  std::string cmd =
      "PID=$(lsof -t -i :" + std::to_string(port) +
      "); if [ ! -z \"$PID\" ]; then kill -9 $PID; fi > /dev/null 2>&1";
  std::system(cmd.c_str());
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

  if (fs::exists(exe_dir / "CONTAINER")) {
    return exe_dir / "CONTAINER";
  }
  if (fs::exists(exe_dir.parent_path() / "CONTAINER")) {
    return exe_dir.parent_path() / "CONTAINER";
  }

  fs::path current = fs::current_path();
  if (fs::exists(current / "CONTAINER")) {
    return current / "CONTAINER";
  }
  if (fs::exists(current.parent_path() / "CONTAINER")) {
    return current.parent_path() / "CONTAINER";
  }

  return exe_dir / "CONTAINER";
}

static void start_backend(Backend *b) {
  if (b->started.load())
    return;

  kill_process_on_port(b->port);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  log_info("[LB] Launching container on port " + std::to_string(b->port) +
           "...");

  auto now = std::chrono::steady_clock::now();
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch())
                .count();
  b->launch_time_ms.store(ms);
  b->started.store(true);

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

// ── Pick Backend (Least Connections + Round-Robin tie breaker)
static Backend *pick_backend() {
  std::vector<Backend *> candidates;
  int min_conn = std::numeric_limits<int>::max();

  for (auto &b : g_backends) {
    if (b->alive.load()) {
      int active = b->active_requests.load();
      if (active < min_conn) {
        min_conn = active;
        candidates.clear();
        candidates.push_back(b.get());
      } else if (active == min_conn) {
        candidates.push_back(b.get());
      }
    }
  }

  if (candidates.empty()) {
    return nullptr;
  }

  if (candidates.size() == 1) {
    return candidates[0];
  }

  // Tie breaker: Round-Robin among candidates
  static std::atomic<size_t> rr_index{0};
  size_t index = rr_index.fetch_add(1, std::memory_order_relaxed);
  return candidates[index % candidates.size()];
}

// ── Health Checker Loop
static void health_checker_loop(int interval_seconds) {
  log_info("Health checker thread started (interval: " +
           std::to_string(interval_seconds) + "s)");

  // 1. Initial startup of target active containers if not in Docker mode
  if (!g_docker_mode.load()) {
    const char *target_active_env = std::getenv("TARGET_ACTIVE_BACKENDS");
    int target_active = target_active_env ? std::stoi(target_active_env) : 3;
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

      // Skip backends that aren't started or have permanently failed
      if (!b->started.load() || b->failed.load()) {
        continue;
      }

      httplib::Client cli(b->host, b->port);
      cli.set_connection_timeout(2, 0); // 2 seconds
      cli.set_read_timeout(2, 0);       // 2 seconds

      auto res = cli.Get("/health");
      bool is_healthy = false;

      if (res && res->status == 200) {
        try {
          auto body_json = json::parse(res->body);
          if (body_json.contains("status") && body_json["status"] == "UP") {
            is_healthy = true;
          }
        } catch (...) {
          // Fail-safe check
          if (res->body.find("\"status\"") != std::string::npos &&
              res->body.find("\"UP\"") != std::string::npos) {
            is_healthy = true;
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
      auto now = std::chrono::steady_clock::now();
      auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch())
                        .count();

      const char *startup_timeout_env = std::getenv("STARTUP_TIMEOUT");
      long long startup_timeout_ms =
          startup_timeout_env ? std::stoll(startup_timeout_env) * 1000 : 10000;

      for (auto &b : g_backends) {
        if (b->started.load() && !b->failed.load()) {
          if (b->alive.load()) {
            b->was_alive.store(true);
          } else {
            bool has_failed = false;
            if (b->was_alive.load()) {
              // It was alive, but now it went DOWN (health checker marked it
              // dead after 2 fails)
              has_failed = true;
              log_error("[LB] Backend " + b->endpoint() +
                        " crashed or stopped responding. Failover triggered.");
            } else {
              // Not alive yet. Check startup timeout
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
              auto f_ms = std::chrono::duration_cast<std::chrono::milliseconds>(f_now.time_since_epoch()).count();
              b->fail_time_ms.store(f_ms);
              kill_process_on_port(b->port);
            }
          }
        }
      }

      // Count active (started & not failed)
      int active_count = 0;
      for (auto &b : g_backends) {
        if (b->started.load() && !b->failed.load()) {
          active_count++;
        }
      }

      const char *target_active_env = std::getenv("TARGET_ACTIVE_BACKENDS");
      int target_active = target_active_env ? std::stoi(target_active_env) : 3;

      if (active_count < target_active) {
        // Collect all inactive / candidate backends
        std::vector<Backend*> candidates;
        for (auto &b : g_backends) {
          if (!b->started.load() || b->failed.load()) {
            candidates.push_back(b.get());
          }
        }

        // Sort candidates: never started first, then failed (oldest fail_time_ms first)
        std::sort(candidates.begin(), candidates.end(), [](Backend *x, Backend *y) {
          bool x_never_started = !x->started.load();
          bool y_never_started = !y->started.load();
          
          if (x_never_started != y_never_started) {
            return x_never_started;
          }
          
          return x->fail_time_ms.load() < y->fail_time_ms.load();
        });

        int needed = target_active - active_count;
        for (int i = 0; i < needed && i < (int)candidates.size(); ++i) {
          Backend *b = candidates[i];
          
          // Reset state parameters
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

// ── Signal Handler for Graceful Shutdown
static void on_signal(int sig) {
  log_info("Shutdown signal (" + std::to_string(sig) + ") received...");
  g_running.store(false);

  if (g_proxy_server) {
    g_proxy_server->stop();
  }
  if (g_admin_server) {
    g_admin_server->stop();
  }

  // Clean up running containers if not in Docker mode
  if (!g_docker_mode.load()) {
    log_info("[LB] Cleaning up running container processes...");
    for (auto &b : g_backends) {
      if (b->started.load()) {
        kill_process_on_port(b->port);
      }
    }
  }
}

int main() {
  g_start_time = std::chrono::steady_clock::now();

  // ── Load Environment Variables
  const char *lb_port_env = std::getenv("LB_PORT");
  int lb_port = lb_port_env ? std::stoi(lb_port_env) : 5649;

  const char *lb_admin_port_env = std::getenv("LB_ADMIN_PORT");
  int lb_admin_port = lb_admin_port_env ? std::stoi(lb_admin_port_env) : 5650;

  const char *backends_env = std::getenv("BACKENDS");
  if (backends_env) {
    g_docker_mode.store(true);
    std::stringstream ss(backends_env);
    std::string item;
    while (std::getline(ss, item, ',')) {
      auto colon = item.find(':');
      if (colon != std::string::npos) {
        try {
          auto b = std::make_unique<Backend>();
          b->host = item.substr(0, colon);
          b->port = std::stoi(item.substr(colon + 1));
          b->alive.store(false);
          b->started.store(true); // Docker Compose manages containers
          g_backends.push_back(std::move(b));
        } catch (...) {
          log_error("Invalid backend port in specification: " + item);
        }
      } else {
        log_error("Invalid backend specification in BACKENDS: " + item);
      }
    }
  } else {
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
        try {
          backend_ports.push_back(std::stoi(item));
        } catch (...) {
          log_error("Invalid backend port in environment: " + item);
        }
      }
    }

    for (int port : backend_ports) {
      auto b = std::make_unique<Backend>();
      b->host = backend_host;
      b->port = port;
      b->alive.store(false);
      b->started.store(false); // Local mode: started dynamically by LB
      g_backends.push_back(std::move(b));
    }
  }

  // ── Set up signal listeners
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  const char *hc_interval_env = std::getenv("HEALTH_CHECK_INTERVAL");
  int hc_interval = hc_interval_env ? std::stoi(hc_interval_env) : 3;

  // ── Start Health Checker Thread
  std::thread hc_thread(health_checker_loop, hc_interval);

  // ── Build Proxy Server
  httplib::Server proxy_svr;
  g_proxy_server = &proxy_svr;

  // Thread pool size: 200, max queued requests: 1000
  proxy_svr.new_task_queue = [] { return new httplib::ThreadPool(200, 1000); };

  // ── Proxy forwarding logic
  // IMPORTANT: We use per-method wildcard handlers (NOT
  // set_pre_routing_handler). set_pre_routing_handler fires BEFORE httplib
  // reads the request body, so req.body is always empty inside it. Method
  // handlers fire AFTER the body has been fully read — this is the correct hook
  // for a body-forwarding proxy.
  auto proxy_handler = [](const httplib::Request &req, httplib::Response &res) {
    Backend *backend = pick_backend();
    if (!backend) {
      json err;
      err["errorCode"] = "ERR_NO_BACKENDS";
      err["message"] =
          "All backends are currently unavailable. Please try again shortly.";
      res.status = 503;
      res.set_content(err.dump(4), "application/json");
      log_error("[LB] Request rejected (503) — no healthy backends for: " +
                req.path);
      return;
    }

    g_total_requests++;
    backend->active_requests++;
    auto start = std::chrono::steady_clock::now();

    // ── Forward to chosen backend
    httplib::Client cli(backend->host, backend->port);
    cli.set_connection_timeout(5, 0);
    cli.set_read_timeout(15, 0);
    cli.set_write_timeout(15, 0);

    httplib::Request fwd;
    fwd.method = req.method;
    fwd.path =
        req.path; // decoded URL path — always populated in method handlers
    fwd.params = req.params; // query-string params — client appends as ?k=v
    fwd.body = req.body; // body is fully read before method handlers fire ✅

    // Forward all headers except connection-control and Host
    for (const auto &h : req.headers) {
      if (h.first == "Host" ||
          h.first == "Content-Length" || // httplib recomputes from body size
          h.first == "Transfer-Encoding") {
        continue;
      }
      fwd.headers.insert(h);
    }
    fwd.headers.emplace("X-LB-Backend", std::to_string(backend->port));

    auto result = cli.send(fwd);

    backend->active_requests--;
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

    if (result) {
      res.status = result->status;

      // Extract Content-Type then use set_content() — links body + type
      // together
      std::string ct = result->has_header("Content-Type")
                           ? result->get_header_value("Content-Type")
                           : "application/json";
      res.set_content(result->body, ct);

      // Forward other response headers (skip the ones set_content already
      // handles)
      for (const auto &h : result->headers) {
        if (h.first == "Content-Length" || h.first == "Content-Type" ||
            h.first == "Transfer-Encoding") {
          continue;
        }
        res.set_header(h.first, h.second);
      }

      // Rolling 10-sample EMA of response time
      int old_avg = backend->avg_response_ms.load();
      backend->avg_response_ms.store(
          old_avg == 0 ? elapsed_ms : (old_avg * 9 + elapsed_ms) / 10);

    } else {
      json e;
      e["errorCode"] = "ERR_BAD_GATEWAY";
      e["message"] = "Bad Gateway: could not reach backend service.";
      res.status = 502;
      res.set_content(e.dump(4), "application/json");
      log_error("[LB] Forward failed → " + backend->endpoint() +
                "  path=" + req.path + "  err=" + to_string(result.error()));
    }
  };

  // Register the proxy handler for every HTTP method using regex wildcard
  proxy_svr.Get(R"(.*)", proxy_handler);
  proxy_svr.Post(R"(.*)", proxy_handler);
  proxy_svr.Put(R"(.*)", proxy_handler);
  proxy_svr.Delete(R"(.*)", proxy_handler);
  proxy_svr.Patch(R"(.*)", proxy_handler);
  proxy_svr.Options(R"(.*)", proxy_handler);

  // ── Build Admin/Status Server
  httplib::Server admin_svr;
  g_admin_server = &admin_svr;

  admin_svr.Get("/lb/health",
                [](const httplib::Request &, httplib::Response &res) {
                  json ok;
                  ok["status"] = "UP";
                  res.status = 200;
                  res.set_content(ok.dump(4), "application/json");
                });

  admin_svr.Get("/lb/status", [](const httplib::Request &,
                                 httplib::Response &res) {
    json status;
    status["lb_status"] = "UP";
    status["total_backends"] = g_backends.size();

    int alive_count = 0;
    json backends_arr = json::array();

    for (const auto &b : g_backends) {
      json b_json;
      b_json["host"] = b->host;
      b_json["port"] = b->port;
      bool is_alive = b->alive.load();
      b_json["alive"] = is_alive;
      b_json["active_requests"] = b->active_requests.load();
      b_json["avg_response_ms"] = b->avg_response_ms.load();
      b_json["consecutive_failures"] = b->consecutive_failures.load();
      b_json["consecutive_successes"] = b->consecutive_successes.load();

      if (is_alive) {
        alive_count++;
      }
      backends_arr.push_back(b_json);
    }

    status["alive_backends"] = alive_count;
    status["backends"] = backends_arr;
    status["total_requests_forwarded"] = g_total_requests.load();

    auto uptime_seconds = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::steady_clock::now() - g_start_time)
                              .count();
    status["uptime_seconds"] = uptime_seconds;

    res.status = 200;
    res.set_content(status.dump(4), "application/json");
  });

  // ── Start Admin Server Thread
  std::thread admin_thread([&]() {
    log_info("Admin HTTP server listening on 0.0.0.0:" +
             std::to_string(lb_admin_port));
    admin_svr.listen("0.0.0.0", lb_admin_port);
    log_info("Admin HTTP server stopped");
  });

  // ── Start Proxy Server (blocking)
  // Build backend list string
  std::string backend_list;
  for (size_t i = 0; i < g_backends.size(); ++i) {
    backend_list += g_backends[i]->endpoint();
    if (i + 1 < g_backends.size())
      backend_list += ", ";
  }

  log_info("C++20 Load Balancer listening on 0.0.0.0:" +
           std::to_string(lb_port));
  std::cout
      << "\n╔══════════════════════════════════════════════════════════╗\n";
  std::cout << "║       Card API Load Balancer  v1.0 – Rohan Sakhare      ║\n";
  std::cout << "╠══════════════════════════════════════════════════════════╣\n";
  std::cout << "║  Proxy Port  : " << lb_port
            << "                                      ║\n";
  std::cout << "║  Admin Port  : " << lb_admin_port
            << "                                      ║\n";
  std::cout << "║  HC Interval : " << hc_interval
            << "s                                      ║\n";
  std::cout << "║  Backends    : " << g_backends.size()
            << " registered                              ║\n";
  for (const auto &b : g_backends) {
    std::cout << "║    → " << b->endpoint()
              << std::string(50 - b->endpoint().size(), ' ') << "║\n";
  }
  std::cout
      << "╚══════════════════════════════════════════════════════════╝\n\n";

  proxy_svr.listen("0.0.0.0", lb_port);

  log_info("Proxy HTTP server stopped");

  // ── Wait for Admin and Health Checker
  if (admin_thread.joinable()) {
    admin_thread.join();
  }
  if (hc_thread.joinable()) {
    hc_thread.join();
  }

  log_info("Load Balancer shutdown complete");
  return 0;
}
