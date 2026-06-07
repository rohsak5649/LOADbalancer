# High-Performance C++20 Reverse Proxy Load Balancer

[![C++ Standard](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/compiler_support/20)
[![Build Status](https://img.shields.io/badge/Build-CMake%20%7C%20Ninja-brightgreen.svg)]()
[![Platform](https://img.shields.io/badge/Platform-Linux%20%7C%20macOS-lightgrey.svg)]()
[![License](https://img.shields.io/badge/License-MIT-gold.svg)](LICENSE)

An industrial-grade, multithreaded **C++20 Reverse Proxy Load Balancer** specifically designed to scale card payment switching API engines (like `CardAPIServer`). It sits on public-facing ports, dynamically intercepting traffic, and balances requests across multiple backend container nodes using a thread-safe, lock-free **Least-Connections** scheduling algorithm with **Round-Robin** fallback tie-breakers.

---

## 📖 Table of Contents
1. [Big Picture Architecture](#-big-picture-architecture)
2. [How It Works Under the Hood](#-how-it-works-under-the-hood)
   * [Least-Connections Routing](#1-least-connections-routing)
   * [Wildcard Method Routing & Request Body Handling](#2-wildcard-method-routing--request-body-handling)
   * [Background Health Checker Daemon](#3-background-health-checker-daemon)
   * [Graceful Shutdown Sequence](#4-graceful-shutdown-sequence)
3. [Deep-Dive Code Walkthrough](#%EF%B8%8F-deep-dive-code-walkthrough)
   * [Thread-Safe Lock-Free Registry Structs](#thread-safe-lock-free-registry-structs)
   * [Decoupled Logging and Timing Helpers](#decoupled-logging-and-timing-helpers)
4. [Setting Up & Running Locally](#%EF%B8%8F-setting-up--running-locally)
   * [Build Requirements](#build-requirements)
   * [Building via CLI (CMake & Ninja)](#building-via-cli-cmake--ninja)
   * [Running in CLion IDE](#running-in-clion-ide)
5. [Docker Orchestrated Setup (5 Backend Nodes + DB + LB)](#-docker-orchestrated-setup-5-backend-nodes--db--lb)
   * [Directory Structure Expectation](#directory-structure-expectation)
   * [Troubleshooting the MySQL Connection Limit](#troubleshooting-the-mysql-connection-limit)
   * [Running the Docker Stack](#running-the-docker-stack)
6. [Monitoring & Administrative APIs](#-monitoring--administrative-apis)
   * [`GET /lb/health`](#get-lbhealth)
   * [`GET /lb/status`](#get-lbstatus)
7. [Simulating and Testing Load Balancing](#%EF%B8%8F-simulating-and-testing-load-balancing)
8. [Troubleshooting Guide](#%EF%B8%8F-troubleshooting-guide)
9. [Author Information](#-author-information)

---

## 🏗️ Big Picture Architecture

```
                  ┌────────────────────────────────────────┐
                  │         EXTERNAL WORLD (Clients)       │
                  │   POS Terminals, Mobile Apps, ATMs,    │
                  │   E-Commerce Gateways, QR Scanners     │
                  └──────────────────┬─────────────────────┘
                                     │
                             HTTP Requests (e.g. POST /transaction/initiate)
                                     │
                                     ▼
         ┌─────────────────────────────────────────────────────────┐
         │                                                         │
         │          LOAD BALANCER CONTAINER (Port 5649)            │
         │                                                         │
         │   ┌─────────────────────────────────────────────────┐   │
         │   │          HEALTH MONITOR (Background)            │   │
         │   │   Pings /health on each backend periodically.   │   │
         │   └─────────────────────────────────────────────────┘   │
         │                                                         │
         │   ┌─────────────────────────────────────────────────┐   │
         │   │          LOAD DECISION ENGINE                   │   │
         │   │   Selects best backend container using:         │   │
         │   │   - Least Connections Algorithm                 │   │
         │   │   - Round-Robin fallback on load tie            │   │
         │   └─────────────────────────────────────────────────┘   │
         │                                                         │
         └───────────┬──────────────────┬──────────────┬───────────┘
                     │                  │              │
          HTTP Forwarding (with X-LB-Backend tracing header)
                     │                  │              │
         ┌───────────▼──┐   ┌───────────▼──┐   ┌───────▼──────┐
         │  BACKEND-1   │   │  BACKEND-2   │   │  BACKEND-5   │
         │  Port: 8080  │   │  Port: 8081  │   │  Port: 8084  │
         │  CardAPI     │   │  CardAPI     │   │  CardAPI     │
         │  Server Node │   │  Server Node │   │  Server Node │
         └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
                │                  │                  │
                └──────────────────┼──────────────────┘
                                   │
                           ┌───────▼───────┐
                           │   Shared DB   │
                           │  MySQL Engine │
                           └───────────────┘
```

Clients send transactions directly to the Load Balancer on port `5649`. The Load Balancer executes its logic, proxies the transaction to a healthy backend, and routes the response back. Backends on ports `8080` to `8084` are isolated internally and communicate via a shared database container.

---

## ⚙️ How It Works Under the Hood

The load balancer manages backend nodes using three parallel asynchronous processes:

### 1. Least-Connections Routing
When an HTTP request hits the proxy:
1. It queries the registry of backends and filters out all nodes marked `alive = false`.
2. Out of the active hosts, it checks the atomic `active_requests` counter.
3. The host processing the lowest number of concurrent requests is selected.
4. **Tie-Breaker Strategy**: If multiple nodes are tied for the lowest load, a global atomic index is incremented to dynamically round-robin the request among the tied hosts.

### 2. Wildcard Method Routing & Request Body Handling
Unlike basic reverse proxies that use pre-routing interceptors (which trigger *before* reading the request body, leaving `req.body` empty), this proxy implements regex wildcard matching for explicit HTTP methods (`Get`, `Post`, `Put`, `Delete`, `Patch`, `Options`).
* This guarantees the request body is **fully read and buffered** before routing.
* The proxy copies all incoming headers (excluding connection-handling headers like `Host`, `Content-Length`, and `Transfer-Encoding`, which are recomputed on the fly).
* An injection header `X-LB-Backend` is populated with the targeted port, simplifying debugging in microservices.

### 3. Background Health Checker Daemon
A dedicated worker thread runs in the background at configurable intervals (default: every 3 seconds):
* It performs a non-blocking `GET /health` call to each backend node.
* It expects a `200 OK` status and a parsed JSON body containing `"status": "UP"`.
* **Flap Prevention System**:
  * If a node fails the health check **2 consecutive times**, it is marked `DEAD` and isolated from routing.
  * If a dead node passes the health check **2 consecutive times**, it is restored to the routing pool.
* State alterations are written using atomic memory structures to prevent race conditions without acquiring heavy OS locks.

### 4. Graceful Shutdown Sequence
Upon capturing termination signals (`SIGINT` or `SIGTERM`):
1. The Load Balancer terminates the incoming HTTP listener immediately.
2. It waits for active proxy threads to complete their current operations.
3. It cleanly terminates the background health checker daemon and joins the threads.
4. Socket descriptors and resources are destroyed with zero memory leakage.

---

## 🖥️ Deep-Dive Code Walkthrough

### Thread-Safe Lock-Free Registry Structs
Every backend node is registered using a dedicated `Backend` struct. All tracking state elements utilize standard C++ atomic wrappers.

```cpp
struct Backend {
  std::string host;
  int port;
  std::atomic<bool> alive{true};
  std::atomic<int> active_requests{0};
  std::atomic<int> consecutive_failures{0};
  std::atomic<int> consecutive_successes{0};
  std::atomic<int> avg_response_ms{0};

  std::string endpoint() const { return host + ":" + std::to_string(port); }
};
```
* Using `std::atomic` variables ensures that incoming HTTP worker threads and the background health checker thread can read and modify states simultaneously without causing race conditions or undefined behavior.
* Registered instances are maintained within a `std::vector<std::unique_ptr<Backend>>`. Storing smart pointers (`unique_ptr`) prevents memory reallocations inside the vector from invoking vector moves that would otherwise copy non-copyable atomic variables.

### Least-Connections Core Implementation
The algorithm executes a single-pass search over the registry to identify candidate backend nodes:

```cpp
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

  if (candidates.empty()) return nullptr;
  if (candidates.size() == 1) return candidates[0];

  // Tie-breaker: Round-Robin selection
  static std::atomic<size_t> rr_index{0};
  size_t index = rr_index.fetch_add(1, std::memory_order_relaxed);
  return candidates[index % candidates.size()];
}
```

### Decoupled Logging and Timing Helpers
Logs include human-readable ISO-like timestamps outputting to `stdout` (for informational trails) and `stderr` (for exceptions and failures):

```cpp
static void log_info(const std::string &msg) {
  auto now = std::chrono::system_clock::now();
  auto in_time_t = std::chrono::system_clock::to_time_t(now);
  std::cout << "[" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S") 
            << "] [INFO] " << msg << std::endl;
}
```

Response times are tracked using a high-precision `std::chrono::steady_clock`. Latencies are stored in the backend registry node as a 10-sample **Exponential Moving Average (EMA)** to provide a rolling reflection of performance:

```cpp
int old_avg = backend->avg_response_ms.load();
backend->avg_response_ms.store(
    old_avg == 0 ? elapsed_ms : (old_avg * 9 + elapsed_ms) / 10
);
```

---

## 🛠️ Setting Up & Running Locally

### Build Requirements
To compile the Load Balancer natively on your machine, you need:
* A compiler supporting **C++20** (GCC 10+, Clang 12+, Xcode 13+)
* **CMake** (v3.15 or higher)
* Native system `pthread` runtime support

### Building via CLI (CMake & Ninja)
Navigate to the directory and run:

```bash
# Generate Ninja build artifacts in a dedicated debug directory
cmake -S . -B cmake-build-debug -G Ninja -DCMAKE_BUILD_TYPE=Debug

# Compile the project 
cmake --build cmake-build-debug

# Run the compiled executable
./cmake-build-debug/LOADbalancer
```

### Running in CLion IDE
1. Open CLion and choose **Open**.
2. Select the directory `/Users/rohansakhare/Desktop/LOADbalancer`.
3. The IDE will automatically read the `CMakeLists.txt` file and load configuration profiles.
4. To configure environment variables, edit the **Run Configuration** settings and populate:
   * `LB_PORT=5649`
   * `LB_ADMIN_PORT=5650`
   * `BACKEND_PORTS=8080,8081,8082,8083,8084`
5. Press the green **Run** button or press `Ctrl + R`.

---

## 🐳 Docker Orchestrated Setup (5 Backend Nodes + DB + LB)

The system is configured to run as a 7-container stack managed via docker-compose.

### Directory Structure Expectation
Your folders should be organized as follows for Docker to build both targets properly:
```
├── CardAPIServer/        # Backend server code (contains main parser&router file)
└── LOADbalancer/         # Load balancer workspace containing docker-compose.yml
```

### Troubleshooting the MySQL Connection Limit
Running 5 backends simultaneously requires a higher MySQL connection limit:
* Each C++ backend spawns an internal database connection pool of **30 sessions** (`POOL_SIZE = 30`).
* **5 instances × 30 connections = 150 database connections.**
* MySQL's default limit is **151** (`max_connections`).
* If you run other client applications (like CLion Database Inspector or MySQL Workbench), the limit is instantly reached, causing the 4th and 5th backend containers to throw database connection errors at startup.

#### **Solution**:
Increase MySQL's connection limit by running this query on your MySQL server:
```sql
SET GLOBAL max_connections = 250;
```

### Running the Docker Stack
Run the orchestrator from the `LOADbalancer/` folder:

```bash
# Start and build the entire environment
docker compose up --build
```

Docker Compose will perform these steps:
1. Start the MySQL database container (`mysql-db`) and wait for it to pass its internal health checks.
2. Spin up the 5 backend container servers (`card-backend-1` to `5`) and wait for their `/health` endpoints to respond with HTTP 200.
3. Build the Load Balancer container (`card-lb`) and connect it to port `5649`.

---

## 📊 Monitoring & Administrative APIs

The Load Balancer hosts an independent administrative HTTP server on port `5650`.

### GET `/lb/health`
Checks the health of the load balancer itself.

* **Request URL**: `http://localhost:5650/lb/health`
* **Response Status**: `200 OK`
* **Response Body**:
```json
{
    "status": "UP"
}
```

### GET `/lb/status`
Exposes the real-time operational status, statistics, latency (EMA), active requests, and uptime for every backend node in the pool.

* **Request URL**: `http://localhost:5650/lb/status`
* **Response Status**: `200 OK`
* **Response Body**:
```json
{
    "lb_status": "UP",
    "total_backends": 5,
    "alive_backends": 5,
    "total_requests_forwarded": 274,
    "uptime_seconds": 124,
    "backends": [
        {
            "host": "127.0.0.1",
            "port": 8080,
            "alive": true,
            "active_requests": 0,
            "avg_response_ms": 14,
            "consecutive_failures": 0,
            "consecutive_successes": 42
        },
        {
            "host": "127.0.0.1",
            "port": 8081,
            "alive": true,
            "active_requests": 0,
            "avg_response_ms": 28,
            "consecutive_failures": 0,
            "consecutive_successes": 42
        },
        {
            "host": "127.0.0.1",
            "port": 8082,
            "alive": true,
            "active_requests": 0,
            "avg_response_ms": 19,
            "consecutive_failures": 0,
            "consecutive_successes": 42
        },
        {
            "host": "127.0.0.1",
            "port": 8083,
            "alive": true,
            "active_requests": 0,
            "avg_response_ms": 22,
            "consecutive_failures": 0,
            "consecutive_successes": 42
        },
        {
            "host": "127.0.0.1",
            "port": 8084,
            "alive": true,
            "active_requests": 0,
            "avg_response_ms": 15,
            "consecutive_failures": 0,
            "consecutive_successes": 42
        }
    ]
}
```

---

## ⚡ Simulating and Testing Load Balancing

You can verify that the load balancer correctly distributes requests across backends by running concurrent transactions.

Using **ApacheBench (ab)**:
```bash
ab -n 1000 -c 10 -p transaction.json -T application/json http://localhost:5649/transaction/initiate
```
*(Where `transaction.json` contains a valid payment payload).*

Querying `/lb/status` while the benchmark is running will show `active_requests` fluctuating across different ports, and the `total_requests_forwarded` increasing across all backends.

---

## 🔧 Troubleshooting Guide

#### 1. Why does my POST request return `400 Bad Request` with an `"Empty request body"` error?
This occurs if the load balancer intercepts requests inside `set_pre_routing_handler`. In `cpp-httplib`, the pre-routing handler runs *before* the server parses the HTTP body, meaning `req.body` is empty. 
* **Fix**: Ensure your Load Balancer routes requests using explicit method handlers (e.g. `proxy_svr.Post(R"(.*)", ...)`) which run *after* the body has been fully buffered in memory.

#### 2. Why does Postman show a blank body but HTTP status `200 OK`?
This happens when copying the backend response body to the frontend response (`res.body = result->body`) without using `res.set_content()`. Without a matching `Content-Type` header, clients (like Postman or web browsers) don't know how to render the response.
* **Fix**: Always set the body and Content-Type together:
  ```cpp
  res.set_content(result->body, "application/json");
  ```

#### 3. Why is there a 10-second delay when sending requests at startup?
If backends default to `alive = true` before their health is verified, the Load Balancer may attempt to route traffic to offline servers. If a server is offline, the proxy client hits a connection timeout (5 seconds per attempt) before failing over to the next host, causing a noticeable delay.
* **Fix**: Configure backends to start as `alive = false`. This ensures that they only receive traffic *after* passing 2 consecutive background health checks.

---

## 👥 Author Information

* **Name**: Rohan Sakhare
* **Email**: [rohanavinashsakhare@gmail.com](mailto:rohanavinashsakhare@gmail.com)
* **Phone**: +91 9112765649
* **GitHub Profile**: [@rohsak5649](https://github.com/rohsak5649)
