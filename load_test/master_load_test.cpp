// // master_load_test.cpp
// #include <iostream>
// #include <thread>
// #include <vector>
// #include <atomic>
// #include <chrono>
// #include <mutex>
// #include <algorithm>
// #include <numeric>
// #include <cstring>
// #include "cpp-httplib/httplib.h"

// struct cache_metadata {
//     char ip_address[16];
//     int port;
//     int start_key;
//     int end_key;
// };

// struct RequestResult {
//     bool success;
//     double latency_ms;
//     std::chrono::steady_clock::time_point timestamp;
// };

// class MasterServerLoadTest {
// private:
//     std::string master_ip;
//     int master_port;
//     int total_requests;
//     int requests_per_second;
//     std::vector<RequestResult> results;
//     std::mutex results_mutex;
//     std::atomic<int> completed_requests{0};

//     void make_request() {
//         auto start = std::chrono::steady_clock::now();
        
//         try {
//             httplib::Client cli(master_ip, master_port);
//             cli.set_connection_timeout(5, 0); // 5 seconds
//             cli.set_read_timeout(5, 0);
            
//             auto res = cli.Get("/initiate");
//             auto end = std::chrono::steady_clock::now();
            
//             double latency = std::chrono::duration<double, std::milli>(end - start).count();
//             bool success = (res && res->status == 200);
            
//             std::lock_guard<std::mutex> lock(results_mutex);
//             results.push_back({success, latency, end});
//         } catch (...) {
//             auto end = std::chrono::steady_clock::now();
//             double latency = std::chrono::duration<double, std::milli>(end - start).count();
            
//             std::lock_guard<std::mutex> lock(results_mutex);
//             results.push_back({false, latency, end});
//         }
        
//         completed_requests++;
//     }

// public:
//     MasterServerLoadTest(const std::string& ip, int port, int total_req, int rps)
//         : master_ip(ip), master_port(port), total_requests(total_req), requests_per_second(rps) {
//         results.reserve(total_requests);
//     }

//     void run_test() {
//         std::cout << "\n=== Starting Master Server Load Test ===" << std::endl;
//         std::cout << "Master Server: " << master_ip << ":" << master_port << std::endl;
//         std::cout << "Total Requests: " << total_requests << std::endl;
//         std::cout << "Target RPS: " << requests_per_second << std::endl;
        
//         auto start_time = std::chrono::steady_clock::now();
        
//         // Calculate interval between requests in microseconds
//         long interval_us = 1000000 / requests_per_second;
        
//         std::vector<std::thread> threads;
//         threads.reserve(total_requests);
        
//         for (int i = 0; i < total_requests; i++) {
//             auto next_request_time = start_time + std::chrono::microseconds(i * interval_us);
            
//             // Sleep until it's time for the next request
//             std::this_thread::sleep_until(next_request_time);
            
//             threads.emplace_back(&MasterServerLoadTest::make_request, this);
            
//             // // Show progress
//             // if ((i + 1) % 100 == 0) {
//             //     std::cout << "Sent: " << (i + 1) << "/" << total_requests << "\r" << std::flush;
//             // }
//         }
        
//         // Wait for all threads to complete
//         std::cout << "\nWaiting for all requests to complete..." << std::endl;
//         for (auto& t : threads) {
//             t.join();
//         }
        
//         auto end_time = std::chrono::steady_clock::now();
//         double duration = std::chrono::duration<double>(end_time - start_time).count();
        
//         print_results(duration);
//     }

// private:
//     void print_results(double duration) {
//         int successful = 0;
//         int failed = 0;
//         std::vector<double> latencies;
        
//         for (const auto& result : results) {
//             if (result.success) {
//                 successful++;
//                 latencies.push_back(result.latency_ms);
//             } else {
//                 failed++;
//             }
//         }
        
//         std::cout << "\n=== Master Server Load Test Results ===" << std::endl;
//         std::cout << "Duration: " << duration << " seconds" << std::endl;
//         std::cout << "Total Requests: " << results.size() << std::endl;
//         std::cout << "Successful: " << successful << std::endl;
//         std::cout << "Failed: " << failed << std::endl;
//         std::cout << "Success Rate: " << (successful * 100.0 / results.size()) << "%" << std::endl;
//         std::cout << "Actual Throughput: " << (successful / duration) << " req/s" << std::endl;
        
//         if (!latencies.empty()) {
//             std::sort(latencies.begin(), latencies.end());
            
//             double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
//             double mean = sum / latencies.size();
//             double median = latencies[latencies.size() / 2];
//             double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
//             double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
            
//             std::cout << "\nLatency Statistics (ms):" << std::endl;
//             std::cout << "  Mean: " << mean << std::endl;
//             std::cout << "  Median: " << median << std::endl;
//             std::cout << "  Min: " << latencies.front() << std::endl;
//             std::cout << "  Max: " << latencies.back() << std::endl;
//             std::cout << "  p95: " << p95 << std::endl;
//             std::cout << "  p99: " << p99 << std::endl;
//         }
//     }
// };

// int main(int argc, char* argv[]) {
//     if (argc < 5) {
//         std::cout << "Usage: " << argv[0] << " <master_ip> <master_port> <total_requests> <rps>" << std::endl;
//         std::cout << "Example: " << argv[0] << " localhost 5050 1000 50" << std::endl;
//         return 1;
//     }

//     std::string master_ip = argv[1];
//     int master_port = std::stoi(argv[2]);
//     int total_requests = std::stoi(argv[3]);
//     int rps = std::stoi(argv[4]);

//     MasterServerLoadTest tester(master_ip, master_port, total_requests, rps);
//     tester.run_test();

//     return 0;
// }


//////////////////////////////////
// #include <iostream>
// #include <thread>
// #include <vector>
// #include <atomic>
// #include <chrono>
// #include <mutex>
// #include <algorithm>
// #include <numeric>
// #include <random>
// #include <queue>
// #include <condition_variable>
// #include "cpp-httplib/httplib.h"

// struct RequestResult {
//     bool success;
//     double latency_ms;
//     std::chrono::steady_clock::time_point timestamp;
// };

// class MasterOpenLoopTest {
// private:
//     std::string master_ip;
//     int master_port;
//     double target_rate;
//     int duration_sec;
//     std::string distribution_type;
//     int num_workers;  // Fixed number of worker threads
    
//     std::vector<RequestResult> results;
//     std::mutex results_mutex;
//     std::atomic<int> requests_sent{0};
//     std::atomic<int> requests_completed{0};
    
//     // Work queue
//     std::queue<int> work_queue;  // Just need to track request IDs
//     std::mutex queue_mutex;
//     std::condition_variable cv;
//     std::atomic<bool> stop_workers{false};

//     std::random_device rd;
//     std::mt19937 gen{rd()};

//     void worker_thread() {
//         while (true) {
//             int request_id;
            
//             {
//                 std::unique_lock<std::mutex> lock(queue_mutex);
//                 cv.wait(lock, [this] { 
//                     return !work_queue.empty() || stop_workers.load(); 
//                 });
                
//                 if (stop_workers.load() && work_queue.empty()) {
//                     return;  // Exit worker
//                 }
                
//                 if (work_queue.empty()) continue;
                
//                 request_id = work_queue.front();
//                 work_queue.pop();
//             }
            
//             // Execute the request
//             send_request();
//         }
//     }

//     void send_request() {
//         auto start = std::chrono::steady_clock::now();
        
//         try {
//             httplib::Client cli(master_ip, master_port);
//             cli.set_connection_timeout(5, 0);
//             cli.set_read_timeout(5, 0);
            
//             auto res = cli.Get("/initiate");
//             auto end = std::chrono::steady_clock::now();
            
//             double latency = std::chrono::duration<double, std::milli>(end - start).count();
//             bool success = (res && res->status == 200);
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({success, latency, end});
//             }
//             requests_completed++;
//         } catch (const std::exception& e) {
//             auto end = std::chrono::steady_clock::now();
//             double latency = std::chrono::duration<double, std::milli>(end - start).count();
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({false, latency, end});
//             }
//             requests_completed++;
//         }
//     }

//     void schedule_request(int request_id) {
//         {
//             std::lock_guard<std::mutex> lock(queue_mutex);
//             work_queue.push(request_id);
//         }
//         cv.notify_one();
//         requests_sent++;
//     }

// public:
//     MasterOpenLoopTest(const std::string& ip, int port, double rate, 
//                        int duration, const std::string& dist = "constant",
//                        int workers = 50)
//         : master_ip(ip), master_port(port), target_rate(rate), 
//           duration_sec(duration), distribution_type(dist), num_workers(workers) {}

//     void run_test() {
//         std::cout << "\n=== Open-Loop Master Server Load Test ===" << std::endl;
//         std::cout << "Master Server: " << master_ip << ":" << master_port << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Duration: " << duration_sec << "s" << std::endl;
//         std::cout << "Distribution: " << distribution_type << std::endl;
//         std::cout << "Worker Threads: " << num_workers << std::endl;
//         std::cout << "Expected Requests: " << (int)(target_rate * duration_sec) << std::endl;
//         std::cout << std::endl;
        
//         // Start worker threads
//         std::vector<std::thread> workers;
//         for (int i = 0; i < num_workers; ++i) {
//             workers.emplace_back(&MasterOpenLoopTest::worker_thread, this);
//         }
        
//         auto test_start = std::chrono::steady_clock::now();
        
//         if (distribution_type == "poisson") {
//             run_poisson();
//         } else {
//             run_constant();
//         }
        
//         auto test_end = std::chrono::steady_clock::now();
//         double actual_duration = std::chrono::duration<double>(test_end - test_start).count();
        
//         // Wait for work queue to drain
//         std::cout << "\nWaiting for work queue to drain..." << std::endl;
//         while (true) {
//             {
//                 std::lock_guard<std::mutex> lock(queue_mutex);
//                 if (work_queue.empty()) break;
//             }
//             std::this_thread::sleep_for(std::chrono::milliseconds(10));
//         }
        
//         // Wait a bit more for in-flight requests
//         std::cout << "Waiting for in-flight requests..." << std::endl;
//         std::this_thread::sleep_for(std::chrono::seconds(5));
        
//         // Stop workers
//         stop_workers.store(true);
//         cv.notify_all();
        
//         for (auto& worker : workers) {
//             worker.join();
//         }
        
//         print_results(actual_duration);
//     }

// private:
//     void run_constant() {
//         double interval = 1.0 / target_rate;
//         int total_requests = static_cast<int>(target_rate * duration_sec);
//         auto start_time = std::chrono::steady_clock::now();
        
//         std::cout << "Sending requests at constant rate..." << std::endl;
        
//         for (int i = 0; i < total_requests; ++i) {
//             auto next_send_time = start_time + 
//                 std::chrono::duration<double>(i * interval);
            
//             std::this_thread::sleep_until(next_send_time);
            
//             schedule_request(i);
            
//             if ((i + 1) % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << (i + 1) << "/" << total_requests 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     void run_poisson() {
//         std::exponential_distribution<> dist(target_rate);
//         auto start_time = std::chrono::steady_clock::now();
//         auto end_time = start_time + std::chrono::seconds(duration_sec);
        
//         std::cout << "Sending requests following Poisson process..." << std::endl;
        
//         int count = 0;
//         while (std::chrono::steady_clock::now() < end_time) {
//             double wait_time = dist(gen);
//             std::this_thread::sleep_for(std::chrono::duration<double>(wait_time));
            
//             schedule_request(count);
//             count++;
            
//             if (count % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << count 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     void print_results(double duration) {
//         if (results.empty()) {
//             std::cout << "No results collected!" << std::endl;
//             return;
//         }
        
//         int successful = 0;
//         int failed = 0;
//         std::vector<double> latencies;
        
//         for (const auto& result : results) {
//             if (result.success) {
//                 successful++;
//                 latencies.push_back(result.latency_ms);
//             } else {
//                 failed++;
//             }
//         }
        
//         std::cout << "\n=== Test Results ===" << std::endl;
//         std::cout << "Actual Duration: " << std::fixed << std::setprecision(2) 
//                  << duration << "s" << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Actual Send Rate: " << (requests_sent.load() / duration) 
//                  << " req/s" << std::endl;
        
//         std::cout << "\nRequest Statistics:" << std::endl;
//         std::cout << "  Total Sent: " << requests_sent.load() << std::endl;
//         std::cout << "  Total Completed: " << results.size() << std::endl;
//         std::cout << "  Successful: " << successful << std::endl;
//         std::cout << "  Failed: " << failed << std::endl;
//         std::cout << "  Success Rate: " << std::fixed << std::setprecision(2)
//                  << (successful * 100.0 / results.size()) << "%" << std::endl;
        
//         std::cout << "\nThroughput:" << std::endl;
//         std::cout << "  Offered Load: " << target_rate << " req/s" << std::endl;
//         std::cout << "  Successful Throughput: " << (successful / duration) 
//                  << " req/s" << std::endl;
//         std::cout << "  Utilization: " << ((successful / duration) / target_rate * 100) 
//                  << "%" << std::endl;
        
//         if (!latencies.empty()) {
//             std::sort(latencies.begin(), latencies.end());
            
//             double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
//             double mean = sum / latencies.size();
//             double median = latencies[latencies.size() / 2];
//             double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
//             double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
            
//             std::cout << "\nLatency Statistics (successful requests, ms):" << std::endl;
//             std::cout << "  Mean: " << mean << std::endl;
//             std::cout << "  Median: " << median << std::endl;
//             std::cout << "  Min: " << latencies.front() << std::endl;
//             std::cout << "  Max: " << latencies.back() << std::endl;
//             std::cout << "  p95: " << p95 << std::endl;
//             std::cout << "  p99: " << p99 << std::endl;
//         }
//     }
// };

// int main(int argc, char* argv[]) {
//     if (argc < 5) {
//         std::cout << "Usage: " << argv[0] << " <master_ip> <master_port> <target_rate> <duration_sec> [distribution] [num_workers]" << std::endl;
//         std::cout << "Distribution: 'constant' (default) or 'poisson'" << std::endl;
//         std::cout << "Example: " << argv[0] << " localhost 5050 100 60 constant 50" << std::endl;
//         return 1;
//     }

//     std::string master_ip = argv[1];
//     int master_port = std::stoi(argv[2]);
//     double target_rate = std::stod(argv[3]);
//     int duration = std::stoi(argv[4]);
//     std::string distribution = (argc > 5) ? argv[5] : "constant";
//     int num_workers = (argc > 6) ? std::stoi(argv[6]) : 50;

//     MasterOpenLoopTest tester(master_ip, master_port, target_rate, duration, distribution, num_workers);
//     tester.run_test();

//     return 0;
// }
////////////////////////////////////////

// #include <iostream>
// #include <thread>
// #include <vector>
// #include <atomic>
// #include <chrono>
// #include <mutex>
// #include <algorithm>
// #include <numeric>
// #include <random>
// #include <queue>
// #include <condition_variable>
// #include <iomanip>
// #include "cpp-httplib/httplib.h"

// struct RequestResult {
//     bool success;
//     double latency_ms;
//     std::chrono::steady_clock::time_point timestamp;
// };

// // FIX 1: Carry the scheduled time in the work item to measure true latency (waiting time + service time)
// struct WorkItem {
//     int id;
//     std::chrono::steady_clock::time_point scheduled_time;
// };

// class MasterOpenLoopTest {
// private:
//     std::string master_ip;
//     int master_port;
//     double target_rate;
//     int duration_sec;
//     std::string distribution_type;
//     int num_workers;
    
//     std::vector<RequestResult> results;
//     std::mutex results_mutex;
//     std::atomic<int> requests_sent{0};
//     std::atomic<int> requests_completed{0};
    
//     std::queue<WorkItem> work_queue;
//     std::mutex queue_mutex;
//     std::condition_variable cv;
//     std::atomic<bool> stop_workers{false};

//     std::random_device rd;
//     std::mt19937 gen{rd()};

//     void worker_thread() {
//         // FIX 2: Create Client ONCE per thread and reuse (Persistent Connection)
//         // This prevents TCP handshake overhead and port exhaustion.
//         httplib::Client cli(master_ip, master_port);
//         cli.set_connection_timeout(5, 0);
//         cli.set_read_timeout(5, 0);
//         cli.set_keep_alive(true);

//         while (true) {
//             WorkItem item;
            
//             {
//                 std::unique_lock<std::mutex> lock(queue_mutex);
//                 cv.wait(lock, [this] { 
//                     return !work_queue.empty() || stop_workers.load(); 
//                 });
                
//                 if (stop_workers.load() && work_queue.empty()) {
//                     return;
//                 }
                
//                 if (work_queue.empty()) continue;
                
//                 item = work_queue.front();
//                 work_queue.pop();
//             }
            
//             // Execute the request using the thread-local client
//             send_request(cli, item.scheduled_time);
//         }
//     }

//     void send_request(httplib::Client& cli, std::chrono::steady_clock::time_point scheduled_time) {
//         try {
//             // Perform the HTTP request
//             auto res = cli.Get("/initiate");
//             auto end = std::chrono::steady_clock::now();
            
//             // FIX 1 (continued): Calculate latency relative to SCHEDULED time.
//             // If the system is overloaded, (end - scheduled_time) will naturally increase
//             // because the request sat in the work_queue waiting for a worker.
//             double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
//             bool success = (res && res->status == 200);
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({success, latency, end});
//             }
//             requests_completed++;
//         } catch (...) {
//             auto end = std::chrono::steady_clock::now();
//             double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({false, latency, end});
//             }
//             requests_completed++;
//         }
//     }

//     void schedule_request(int request_id, std::chrono::steady_clock::time_point scheduled_time) {
//         {
//             std::lock_guard<std::mutex> lock(queue_mutex);
//             work_queue.push({request_id, scheduled_time});
//         }
//         cv.notify_one();
//         requests_sent++;
//     }

// public:
//     MasterOpenLoopTest(const std::string& ip, int port, double rate, 
//                        int duration, const std::string& dist = "constant",
//                        int workers = 50)
//         : master_ip(ip), master_port(port), target_rate(rate), 
//           duration_sec(duration), distribution_type(dist), num_workers(workers) {}

//     void run_test() {
//         std::cout << "\n=== Open-Loop Master Server Load Test (Corrected) ===" << std::endl;
//         std::cout << "Master Server: " << master_ip << ":" << master_port << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Duration: " << duration_sec << "s" << std::endl;
//         std::cout << "Distribution: " << distribution_type << std::endl;
//         std::cout << "Worker Threads: " << num_workers << std::endl;
//         std::cout << "Expected Requests: " << (int)(target_rate * duration_sec) << std::endl;
//         std::cout << std::endl;
        
//         // Start worker threads
//         std::vector<std::thread> workers;
//         for (int i = 0; i < num_workers; ++i) {
//             workers.emplace_back(&MasterOpenLoopTest::worker_thread, this);
//         }
        
//         auto test_start = std::chrono::steady_clock::now();
        
//         if (distribution_type == "poisson") {
//             run_poisson();
//         } else {
//             run_constant();
//         }
        
//         auto test_end = std::chrono::steady_clock::now();
//         double actual_duration = std::chrono::duration<double>(test_end - test_start).count();
        
//         // Wait for work queue to drain
//         std::cout << "\nWaiting for work queue to drain..." << std::endl;
//         while (true) {
//             {
//                 std::lock_guard<std::mutex> lock(queue_mutex);
//                 if (work_queue.empty()) break;
//             }
//             std::this_thread::sleep_for(std::chrono::milliseconds(10));
//         }
        
//         // Wait a bit more for in-flight requests
//         std::cout << "Waiting for in-flight requests..." << std::endl;
//         std::this_thread::sleep_for(std::chrono::seconds(2));
        
//         // Stop workers
//         stop_workers.store(true);
//         cv.notify_all();
        
//         for (auto& worker : workers) {
//             worker.join();
//         }
        
//         print_results(actual_duration);
//     }

// private:
//     void run_constant() {
//         double interval_seconds = 1.0 / target_rate;
//         int total_requests = static_cast<int>(target_rate * duration_sec);
        
//         // FIX 3: Use a virtual timeline to prevent drift
//         auto next_schedule_time = std::chrono::steady_clock::now();
        
//         std::cout << "Sending requests at constant rate..." << std::endl;
        
//         for (int i = 0; i < total_requests; ++i) {
//             // Increment virtual timeline
//             next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
//                 std::chrono::duration<double>(interval_seconds)
//             );
            
//             // Sleep until the exact scheduled moment
//             std::this_thread::sleep_until(next_schedule_time);
            
//             // Pass the exact time this SHOULD have run
//             schedule_request(i, next_schedule_time);
            
//             if ((i + 1) % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << (i + 1) << "/" << total_requests 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     void run_poisson() {
//         std::exponential_distribution<> dist(target_rate);
        
//         // FIX 3: Use virtual timeline here as well
//         auto next_schedule_time = std::chrono::steady_clock::now();
//         auto end_time = next_schedule_time + std::chrono::seconds(duration_sec);
        
//         std::cout << "Sending requests following Poisson process..." << std::endl;
        
//         int count = 0;
//         while (next_schedule_time < end_time) {
//             double wait_time = dist(gen);
            
//             // Advance virtual timeline
//             next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
//                 std::chrono::duration<double>(wait_time)
//             );
            
//             // Only sleep if we haven't already fallen behind the virtual timeline
//             if (next_schedule_time > std::chrono::steady_clock::now()) {
//                  std::this_thread::sleep_until(next_schedule_time);
//             }

//             schedule_request(count, next_schedule_time);
//             count++;
            
//             if (count % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << count 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     void print_results(double duration) {
//         if (results.empty()) {
//             std::cout << "No results collected!" << std::endl;
//             return;
//         }
        
//         int successful = 0;
//         int failed = 0;
//         std::vector<double> latencies;
        
//         for (const auto& result : results) {
//             if (result.success) {
//                 successful++;
//                 latencies.push_back(result.latency_ms);
//             } else {
//                 failed++;
//             }
//         }
        
//         std::cout << "\n=== Test Results ===" << std::endl;
//         std::cout << "Actual Duration: " << std::fixed << std::setprecision(2) 
//                  << duration << "s" << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Actual Send Rate: " << (requests_sent.load() / duration) 
//                  << " req/s" << std::endl;
        
//         std::cout << "\nRequest Statistics:" << std::endl;
//         std::cout << "  Total Sent: " << requests_sent.load() << std::endl;
//         std::cout << "  Total Completed: " << results.size() << std::endl;
//         std::cout << "  Successful: " << successful << std::endl;
//         std::cout << "  Failed: " << failed << std::endl;
//         std::cout << "  Success Rate: " << std::fixed << std::setprecision(2)
//                  << (successful * 100.0 / results.size()) << "%" << std::endl;
        
//         std::cout << "\nThroughput:" << std::endl;
//         std::cout << "  Offered Load: " << target_rate << " req/s" << std::endl;
//         std::cout << "  Successful Throughput: " << (successful / duration) 
//                  << " req/s" << std::endl;
        
//         if (!latencies.empty()) {
//             std::sort(latencies.begin(), latencies.end());
            
//             double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
//             double mean = sum / latencies.size();
//             double median = latencies[latencies.size() / 2];
//             double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
//             double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
//             double p999 = latencies[static_cast<size_t>(latencies.size() * 0.999)];
            
//             std::cout << "\nLatency Statistics (successful requests, ms):" << std::endl;
//             std::cout << "  Mean:   " << mean << std::endl;
//             std::cout << "  Median: " << median << std::endl;
//             std::cout << "  Min:    " << latencies.front() << std::endl;
//             std::cout << "  Max:    " << latencies.back() << std::endl;
//             std::cout << "  p95:    " << p95 << std::endl;
//             std::cout << "  p99:    " << p99 << std::endl;
//             std::cout << "  p99.9:  " << p999 << std::endl;
//         }
//     }
// };

// int main(int argc, char* argv[]) {
//     if (argc < 5) {
//         std::cout << "Usage: " << argv[0] << " <master_ip> <master_port> <target_rate> <duration_sec> [distribution] [num_workers]" << std::endl;
//         std::cout << "Distribution: 'constant' (default) or 'poisson'" << std::endl;
//         std::cout << "Example: " << argv[0] << " localhost 5050 100 60 constant 50" << std::endl;
//         return 1;
//     }

//     std::string master_ip = argv[1];
//     int master_port = std::stoi(argv[2]);
//     double target_rate = std::stod(argv[3]);
//     int duration = std::stoi(argv[4]);
//     std::string distribution = (argc > 5) ? argv[5] : "constant";
//     int num_workers = (argc > 6) ? std::stoi(argv[6]) : 50;

//     MasterOpenLoopTest tester(master_ip, master_port, target_rate, duration, distribution, num_workers);
//     tester.run_test();

//     return 0;
// }

// #include <iostream>
// #include <thread>
// #include <vector>
// #include <atomic>
// #include <chrono>
// #include <mutex>
// #include <algorithm>
// #include <numeric>
// #include <random>
// #include <queue>
// #include <condition_variable>
// #include <iomanip>
// #include <fstream>
// #include "cpp-httplib/httplib.h"

// struct RequestResult {
//     bool success;
//     double latency_ms;
//     std::chrono::steady_clock::time_point timestamp;
//     std::string error_msg;  // Added for debugging
// };

// struct WorkItem {
//     int id;
//     std::chrono::steady_clock::time_point scheduled_time;
// };

// class MasterOpenLoopTest {
// private:
//     std::string master_ip;
//     int master_port;
//     double target_rate;
//     int duration_sec;
//     std::string distribution_type;
//     int num_workers;
    
//     std::vector<RequestResult> results;
//     std::mutex results_mutex;
//     std::atomic<int> requests_sent{0};
//     std::atomic<int> requests_completed{0};
    
//     std::queue<WorkItem> work_queue;
//     std::mutex queue_mutex;
//     std::condition_variable cv;
//     std::atomic<bool> stop_workers{false};

//     std::random_device rd;
//     std::mt19937 gen{rd()};

//     void worker_thread() {
//         // Create Client ONCE per thread and reuse
//         httplib::Client cli(master_ip, master_port);
//         cli.set_connection_timeout(5, 0);
//         cli.set_read_timeout(5, 0);
//         cli.set_keep_alive(true);

//         while (true) {
//             WorkItem item;
            
//             {
//                 std::unique_lock<std::mutex> lock(queue_mutex);
//                 cv.wait(lock, [this] { 
//                     return !work_queue.empty() || stop_workers.load(); 
//                 });
                
//                 if (stop_workers.load() && work_queue.empty()) {
//                     return;
//                 }
                
//                 if (work_queue.empty()) continue;
                
//                 item = work_queue.front();
//                 work_queue.pop();
//             }
            
//             send_request(cli, item.scheduled_time);
//         }
//     }

//     void send_request(httplib::Client& cli, std::chrono::steady_clock::time_point scheduled_time) {
//         auto start = std::chrono::steady_clock::now();
        
//         try {
//             auto res = cli.Get("/initiate");
//             auto end = std::chrono::steady_clock::now();
            
//             double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
//             if (res) {
//                 bool success = (res->status == 200);
//                 std::string error_msg = success ? "" : "HTTP " + std::to_string(res->status);
                
//                 {
//                     std::lock_guard<std::mutex> lock(results_mutex);
//                     results.push_back({success, latency, end, error_msg});
//                 }
//                 requests_completed++;
                
//                 // Debug: Print first few failures
//                 if (!success && requests_completed.load() <= 10) {
//                     std::cout << "\n[DEBUG] Request failed: HTTP " << res->status << std::endl;
//                 }
//             } else {
//                 // Connection failed
//                 auto err = res.error();
//                 std::string error_msg = "Connection error: " + std::to_string(static_cast<int>(err));
                
//                 {
//                     std::lock_guard<std::mutex> lock(results_mutex);
//                     results.push_back({false, latency, end, error_msg});
//                 }
//                 requests_completed++;
                
//                 // Debug: Print first few connection errors
//                 if (requests_completed.load() <= 10) {
//                     std::cout << "\n[DEBUG] Connection failed: " << error_msg << std::endl;
//                 }
//             }
//         } catch (const std::exception& e) {
//             auto end = std::chrono::steady_clock::now();
//             double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
//             std::string error_msg = std::string("Exception: ") + e.what();
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({false, latency, end, error_msg});
//             }
//             requests_completed++;
            
//             // Debug: Print first few exceptions
//             if (requests_completed.load() <= 10) {
//                 std::cout << "\n[DEBUG] Exception: " << e.what() << std::endl;
//             }
//         } catch (...) {
//             auto end = std::chrono::steady_clock::now();
//             double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
//             {
//                 std::lock_guard<std::mutex> lock(results_mutex);
//                 results.push_back({false, latency, end, "Unknown exception"});
//             }
//             requests_completed++;
            
//             if (requests_completed.load() <= 10) {
//                 std::cout << "\n[DEBUG] Unknown exception occurred" << std::endl;
//             }
//         }
//     }

//     void schedule_request(int request_id, std::chrono::steady_clock::time_point scheduled_time) {
//         {
//             std::lock_guard<std::mutex> lock(queue_mutex);
//             work_queue.push({request_id, scheduled_time});
//         }
//         cv.notify_one();
//         requests_sent++;
//     }

// public:
//     MasterOpenLoopTest(const std::string& ip, int port, double rate, 
//                        int duration, const std::string& dist = "constant",
//                        int workers = 50)
//         : master_ip(ip), master_port(port), target_rate(rate), 
//           duration_sec(duration), distribution_type(dist), num_workers(workers) {}

//     void run_test() {
//         std::cout << "\n=== Open-Loop Master Server Load Test ===" << std::endl;
//         std::cout << "Master Server: " << master_ip << ":" << master_port << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Duration: " << duration_sec << "s" << std::endl;
//         std::cout << "Distribution: " << distribution_type << std::endl;
//         std::cout << "Worker Threads: " << num_workers << std::endl;
//         std::cout << "Expected Requests: " << (int)(target_rate * duration_sec) << std::endl;
        
//         // Test connection first
//         std::cout << "\nTesting connection to master server..." << std::endl;
//         httplib::Client test_cli(master_ip, master_port);
//         test_cli.set_connection_timeout(5, 0);
//         auto test_res = test_cli.Get("/initiate");
        
//         if (!test_res) {
//             std::cout << "ERROR: Cannot connect to master server!" << std::endl;
//             std::cout << "Error: " << to_string(test_res.error()) << std::endl;
//             std::cout << "Please check if master server is running at " << master_ip << ":" << master_port << std::endl;
//             return;
//         }
        
//         if (test_res->status != 200) {
//             std::cout << "ERROR: Master server returned HTTP " << test_res->status << std::endl;
//             std::cout << "Response body: " << test_res->body << std::endl;
//             return;
//         }
        
//         std::cout << "Connection test successful! Starting load test..." << std::endl;
//         std::cout << std::endl;
        
//         // Start worker threads
//         std::vector<std::thread> workers;
//         for (int i = 0; i < num_workers; ++i) {
//             workers.emplace_back(&MasterOpenLoopTest::worker_thread, this);
//         }
        
//         auto test_start = std::chrono::steady_clock::now();
        
//         if (distribution_type == "poisson") {
//             run_poisson();
//         } else {
//             run_constant();
//         }
        
//         auto test_end = std::chrono::steady_clock::now();
//         double actual_duration = std::chrono::duration<double>(test_end - test_start).count();
        
//         // Wait for work queue to drain
//         std::cout << "\nWaiting for work queue to drain..." << std::endl;
//         int wait_count = 0;
//         while (wait_count < 100) {
//             {
//                 std::lock_guard<std::mutex> lock(queue_mutex);
//                 if (work_queue.empty()) break;
//             }
//             std::this_thread::sleep_for(std::chrono::milliseconds(100));
//             wait_count++;
//         }
        
//         // Wait for in-flight requests
//         std::cout << "Waiting for in-flight requests..." << std::endl;
//         std::this_thread::sleep_for(std::chrono::seconds(5));
        
//         // Stop workers
//         stop_workers.store(true);
//         cv.notify_all();
        
//         for (auto& worker : workers) {
//             worker.join();
//         }
        
//         print_results(actual_duration);
//     }

//     void save_detailed_results(const std::string& filename) {
//         std::ofstream file(filename);
//         file << "timestamp_ms,latency_ms,success,error\n";
        
//         auto test_start = results.empty() ? std::chrono::steady_clock::now() : results[0].timestamp;
        
//         for (const auto& result : results) {
//             double timestamp = std::chrono::duration<double, std::milli>(
//                 result.timestamp - test_start).count();
//             file << std::fixed << std::setprecision(3)
//                  << timestamp << ","
//                  << result.latency_ms << ","
//                  << (result.success ? 1 : 0) << ","
//                  << result.error_msg << "\n";
//         }
        
//         file.close();
//         std::cout << "Detailed results saved to: " << filename << std::endl;
//     }

// private:
//     void run_constant() {
//         double interval_seconds = 1.0 / target_rate;
//         int total_requests = static_cast<int>(target_rate * duration_sec);
        
//         auto next_schedule_time = std::chrono::steady_clock::now();
        
//         std::cout << "Sending requests at constant rate..." << std::endl;
        
//         for (int i = 0; i < total_requests; ++i) {
//             next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
//                 std::chrono::duration<double>(interval_seconds)
//             );
            
//             std::this_thread::sleep_until(next_schedule_time);
            
//             schedule_request(i, next_schedule_time);
            
//             if ((i + 1) % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << (i + 1) << "/" << total_requests 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     void run_poisson() {
//         std::exponential_distribution<> dist(target_rate);
        
//         auto next_schedule_time = std::chrono::steady_clock::now();
//         auto end_time = next_schedule_time + std::chrono::seconds(duration_sec);
        
//         std::cout << "Sending requests following Poisson process..." << std::endl;
        
//         int count = 0;
//         while (next_schedule_time < end_time) {
//             double wait_time = dist(gen);
            
//             next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
//                 std::chrono::duration<double>(wait_time)
//             );
            
//             if (next_schedule_time > std::chrono::steady_clock::now()) {
//                  std::this_thread::sleep_until(next_schedule_time);
//             }

//             schedule_request(count, next_schedule_time);
//             count++;
            
//             if (count % 100 == 0) {
//                 int queue_size;
//                 {
//                     std::lock_guard<std::mutex> lock(queue_mutex);
//                     queue_size = work_queue.size();
//                 }
//                 std::cout << "Sent: " << count 
//                          << " | Completed: " << requests_completed.load()
//                          << " | Queue: " << queue_size
//                          << "\r" << std::flush;
//             }
//         }
//         std::cout << std::endl;
//     }

//     std::string to_string(httplib::Error err) {
//         switch(err) {
//             case httplib::Error::Success: return "Success";
//             case httplib::Error::Connection: return "Connection error";
//             case httplib::Error::BindIPAddress: return "Bind IP address error";
//             case httplib::Error::Read: return "Read error";
//             case httplib::Error::Write: return "Write error";
//             case httplib::Error::ExceedRedirectCount: return "Exceed redirect count";
//             case httplib::Error::Canceled: return "Canceled";
//             case httplib::Error::SSLConnection: return "SSL connection error";
//             case httplib::Error::SSLLoadingCerts: return "SSL loading certs error";
//             case httplib::Error::SSLServerVerification: return "SSL server verification error";
//             case httplib::Error::UnsupportedMultipartBoundaryChars: return "Unsupported multipart boundary chars";
//             case httplib::Error::Compression: return "Compression error";
//             default: return "Unknown error";
//         }
//     }

//     void print_results(double duration) {
//         if (results.empty()) {
//             std::cout << "\nERROR: No results collected!" << std::endl;
//             return;
//         }
        
//         int successful = 0;
//         int failed = 0;
//         std::vector<double> latencies;
//         std::map<std::string, int> error_counts;
        
//         for (const auto& result : results) {
//             if (result.success) {
//                 successful++;
//                 latencies.push_back(result.latency_ms);
//             } else {
//                 failed++;
//                 error_counts[result.error_msg]++;
//             }
//         }
        
//         std::cout << "\n=== Test Results ===" << std::endl;
//         std::cout << "Actual Duration: " << std::fixed << std::setprecision(2) 
//                  << duration << "s" << std::endl;
//         std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
//         std::cout << "Actual Send Rate: " << (requests_sent.load() / duration) 
//                  << " req/s" << std::endl;
        
//         std::cout << "\nRequest Statistics:" << std::endl;
//         std::cout << "  Total Sent: " << requests_sent.load() << std::endl;
//         std::cout << "  Total Completed: " << results.size() << std::endl;
//         std::cout << "  Successful: " << successful << std::endl;
//         std::cout << "  Failed: " << failed << std::endl;
//         std::cout << "  Success Rate: " << std::fixed << std::setprecision(2)
//                  << (successful * 100.0 / results.size()) << "%" << std::endl;
        
//         if (failed > 0) {
//             std::cout << "\nError Breakdown:" << std::endl;
//             for (const auto& [error, count] : error_counts) {
//                 std::cout << "  " << error << ": " << count << std::endl;
//             }
//         }
        
//         std::cout << "\nThroughput:" << std::endl;
//         std::cout << "  Offered Load: " << target_rate << " req/s" << std::endl;
//         std::cout << "  Successful Throughput: " << (successful / duration) 
//                  << " req/s" << std::endl;
        
//         if (successful > 0) {
//             double utilization = (successful / duration) / target_rate * 100;
//             std::cout << "  Utilization: " << utilization << "%" << std::endl;
//         }
        
//         if (!latencies.empty()) {
//             std::sort(latencies.begin(), latencies.end());
            
//             double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
//             double mean = sum / latencies.size();
//             double median = latencies[latencies.size() / 2];
//             double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
//             double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
            
//             std::cout << "\nLatency Statistics (successful requests, ms):" << std::endl;
//             std::cout << "  Mean:   " << mean << std::endl;
//             std::cout << "  Median: " << median << std::endl;
//             std::cout << "  Min:    " << latencies.front() << std::endl;
//             std::cout << "  Max:    " << latencies.back() << std::endl;
//             std::cout << "  p95:    " << p95 << std::endl;
//             std::cout << "  p99:    " << p99 << std::endl;
//         }
        
//         save_detailed_results("load_test_detailed.csv");
//     }
// };

// int main(int argc, char* argv[]) {
//     if (argc < 5) {
//         std::cout << "Usage: " << argv[0] << " <master_ip> <master_port> <target_rate> <duration_sec> [distribution] [num_workers]" << std::endl;
//         std::cout << "Distribution: 'constant' (default) or 'poisson'" << std::endl;
//         std::cout << "Example: " << argv[0] << " localhost 5050 100 60 constant 50" << std::endl;
//         return 1;
//     }

//     std::string master_ip = argv[1];
//     int master_port = std::stoi(argv[2]);
//     double target_rate = std::stod(argv[3]);
//     int duration = std::stoi(argv[4]);
//     std::string distribution = (argc > 5) ? argv[5] : "constant";
//     int num_workers = (argc > 6) ? std::stoi(argv[6]) : 50;

//     MasterOpenLoopTest tester(master_ip, master_port, target_rate, duration, distribution, num_workers);
//     tester.run_test();

//     return 0;
// }

#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <random>
#include <iomanip>
#include <fstream>
#include "cpp-httplib/httplib.h"

struct RequestResult {
    bool success;
    double latency_ms;
    std::chrono::steady_clock::time_point timestamp;
    std::string error_msg;
};

class MasterOpenLoopTest {
private:
    std::string master_ip;
    int master_port;
    double target_rate;
    int duration_sec;
    std::string distribution_type;
    
    std::vector<RequestResult> results;
    std::mutex results_mutex;
    std::atomic<int> requests_sent{0};
    std::atomic<int> requests_completed{0};
    std::atomic<int> active_threads{0};

    std::random_device rd;
    std::mt19937 gen{rd()};
    
    // Keep track of threads (but we'll detach them)
    std::atomic<int> max_concurrent_threads{0};

    void send_request_async(std::chrono::steady_clock::time_point scheduled_time, int request_id) {
        active_threads++;
        
        // Update max concurrent threads
        int current = active_threads.load();
        int max = max_concurrent_threads.load();
        while (current > max && !max_concurrent_threads.compare_exchange_weak(max, current)) {
            max = max_concurrent_threads.load();
        }
        
        auto start = std::chrono::steady_clock::now();
        
        try {
            // Create a new client for each request
            httplib::Client cli(master_ip, master_port);
            cli.set_connection_timeout(5, 0);
            cli.set_read_timeout(5, 0);
            
            auto res = cli.Get("/initiate");
            auto end = std::chrono::steady_clock::now();
            
            double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
            if (res) {
                bool success = (res->status == 200);
                std::string error_msg = success ? "" : "HTTP " + std::to_string(res->status);
                
                {
                    std::lock_guard<std::mutex> lock(results_mutex);
                    results.push_back({success, latency, end, error_msg});
                }
                requests_completed++;
                
                if (!success && requests_completed.load() <= 10) {
                    std::cout << "\n[DEBUG] Request " << request_id << " failed: HTTP " << res->status << std::endl;
                }
            } else {
                auto err = res.error();
                std::string error_msg = "Connection error: " + to_string(err);
                
                {
                    std::lock_guard<std::mutex> lock(results_mutex);
                    results.push_back({false, latency, end, error_msg});
                }
                requests_completed++;
                
                if (requests_completed.load() <= 10) {
                    std::cout << "\n[DEBUG] Request " << request_id << " connection failed: " << error_msg << std::endl;
                }
            }
        } catch (const std::exception& e) {
            auto end = std::chrono::steady_clock::now();
            double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
            std::string error_msg = std::string("Exception: ") + e.what();
            
            {
                std::lock_guard<std::mutex> lock(results_mutex);
                results.push_back({false, latency, end, error_msg});
            }
            requests_completed++;
            
            if (requests_completed.load() <= 10) {
                std::cout << "\n[DEBUG] Request " << request_id << " exception: " << e.what() << std::endl;
            }
        } catch (...) {
            auto end = std::chrono::steady_clock::now();
            double latency = std::chrono::duration<double, std::milli>(end - scheduled_time).count();
            
            {
                std::lock_guard<std::mutex> lock(results_mutex);
                results.push_back({false, latency, end, "Unknown exception"});
            }
            requests_completed++;
        }
        
        active_threads--;
    }

public:
    MasterOpenLoopTest(const std::string& ip, int port, double rate, 
                       int duration, const std::string& dist = "constant")
        : master_ip(ip), master_port(port), target_rate(rate), 
          duration_sec(duration), distribution_type(dist) {}

    void run_test() {
        std::cout << "\n=== Open-Loop Master Server Load Test (Async Threads) ===" << std::endl;
        std::cout << "Master Server: " << master_ip << ":" << master_port << std::endl;
        std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
        std::cout << "Duration: " << duration_sec << "s" << std::endl;
        std::cout << "Distribution: " << distribution_type << std::endl;
        std::cout << "Expected Requests: " << (int)(target_rate * duration_sec) << std::endl;
        std::cout << "\nNOTE: Each request creates a new thread (detached)" << std::endl;
        std::cout << "Ensure ulimit -u is set high enough!" << std::endl;
        
        // Test connection first
        std::cout << "\nTesting connection to master server..." << std::endl;
        httplib::Client test_cli(master_ip, master_port);
        test_cli.set_connection_timeout(5, 0);
        auto test_res = test_cli.Get("/initiate");
        
        if (!test_res) {
            std::cout << "ERROR: Cannot connect to master server!" << std::endl;
            std::cout << "Error: " << to_string(test_res.error()) << std::endl;
            return;
        }
        
        if (test_res->status != 200) {
            std::cout << "ERROR: Master server returned HTTP " << test_res->status << std::endl;
            return;
        }
        
        std::cout << "Connection test successful! Starting load test..." << std::endl;
        std::cout << std::endl;
        
        auto test_start = std::chrono::steady_clock::now();
        
        if (distribution_type == "poisson") {
            run_poisson();
        } else {
            run_constant();
        }
        
        auto test_end = std::chrono::steady_clock::now();
        double actual_duration = std::chrono::duration<double>(test_end - test_start).count();
        
        // Wait for all requests to complete
        std::cout << "\nWaiting for all requests to complete..." << std::endl;
        std::cout << "Sent: " << requests_sent.load() 
                 << " | Completed: " << requests_completed.load() 
                 << " | Active threads: " << active_threads.load() << std::endl;
        
        int wait_count = 0;
        while (requests_completed.load() < requests_sent.load() && wait_count < 600) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            wait_count++;
            
            if (wait_count % 10 == 0) {
                std::cout << "Waiting... Completed: " << requests_completed.load() 
                         << "/" << requests_sent.load() 
                         << " | Active: " << active_threads.load() << "\r" << std::flush;
            }
        }
        std::cout << std::endl;
        
        print_results(actual_duration);
    }

    void save_detailed_results(const std::string& filename) {
        std::ofstream file(filename);
        file << "timestamp_ms,latency_ms,success,error\n";
        
        auto test_start = results.empty() ? std::chrono::steady_clock::now() : results[0].timestamp;
        
        for (const auto& result : results) {
            double timestamp = std::chrono::duration<double, std::milli>(
                result.timestamp - test_start).count();
            file << std::fixed << std::setprecision(3)
                 << timestamp << ","
                 << result.latency_ms << ","
                 << (result.success ? 1 : 0) << ","
                 << result.error_msg << "\n";
        }
        
        file.close();
        std::cout << "Detailed results saved to: " << filename << std::endl;
    }

private:
    void run_constant() {
        double interval_seconds = 1.0 / target_rate;
        int total_requests = static_cast<int>(target_rate * duration_sec);
        
        auto next_schedule_time = std::chrono::steady_clock::now();
        
        std::cout << "Sending requests at constant rate..." << std::endl;
        
        for (int i = 0; i < total_requests; ++i) {
            next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(interval_seconds)
            );
            
            std::this_thread::sleep_until(next_schedule_time);
            
            // Create detached thread for this request
            std::thread request_thread(&MasterOpenLoopTest::send_request_async, 
                                      this, next_schedule_time, i);
            request_thread.detach();
            
            requests_sent++;
            
            if ((i + 1) % 100 == 0) {
                std::cout << "Sent: " << (i + 1) << "/" << total_requests 
                         << " | Completed: " << requests_completed.load()
                         << " | Active threads: " << active_threads.load()
                         << " | Max concurrent: " << max_concurrent_threads.load()
                         << "\r" << std::flush;
            }
        }
        std::cout << std::endl;
    }

    void run_poisson() {
        std::exponential_distribution<> dist(target_rate);
        
        auto next_schedule_time = std::chrono::steady_clock::now();
        auto end_time = next_schedule_time + std::chrono::seconds(duration_sec);
        
        std::cout << "Sending requests following Poisson process..." << std::endl;
        
        int count = 0;
        while (next_schedule_time < end_time) {
            double wait_time = dist(gen);
            
            next_schedule_time += std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                std::chrono::duration<double>(wait_time)
            );
            
            if (next_schedule_time > std::chrono::steady_clock::now()) {
                 std::this_thread::sleep_until(next_schedule_time);
            }

            // Create detached thread for this request
            std::thread request_thread(&MasterOpenLoopTest::send_request_async, 
                                      this, next_schedule_time, count);
            request_thread.detach();
            
            requests_sent++;
            count++;
            
            if (count % 100 == 0) {
                std::cout << "Sent: " << count 
                         << " | Completed: " << requests_completed.load()
                         << " | Active threads: " << active_threads.load()
                         << " | Max concurrent: " << max_concurrent_threads.load()
                         << "\r" << std::flush;
            }
        }
        std::cout << std::endl;
    }

    std::string to_string(httplib::Error err) {
        switch(err) {
            case httplib::Error::Success: return "Success";
            case httplib::Error::Connection: return "Connection error";
            case httplib::Error::BindIPAddress: return "Bind IP address error";
            case httplib::Error::Read: return "Read error";
            case httplib::Error::Write: return "Write error";
            case httplib::Error::ExceedRedirectCount: return "Exceed redirect count";
            case httplib::Error::Canceled: return "Canceled";
            case httplib::Error::SSLConnection: return "SSL connection error";
            case httplib::Error::SSLLoadingCerts: return "SSL loading certs error";
            case httplib::Error::SSLServerVerification: return "SSL server verification error";
            case httplib::Error::UnsupportedMultipartBoundaryChars: return "Unsupported multipart boundary chars";
            case httplib::Error::Compression: return "Compression error";
            default: return "Unknown error";
        }
    }

    void print_results(double duration) {
        if (results.empty()) {
            std::cout << "\nERROR: No results collected!" << std::endl;
            return;
        }
        
        int successful = 0;
        int failed = 0;
        std::vector<double> latencies;
        std::map<std::string, int> error_counts;
        
        for (const auto& result : results) {
            if (result.success) {
                successful++;
                latencies.push_back(result.latency_ms);
            } else {
                failed++;
                error_counts[result.error_msg]++;
            }
        }
        
        std::cout << "\n=== Test Results ===" << std::endl;
        std::cout << "Actual Duration: " << std::fixed << std::setprecision(2) 
                 << duration << "s" << std::endl;
        std::cout << "Target Rate: " << target_rate << " req/s" << std::endl;
        std::cout << "Actual Send Rate: " << (requests_sent.load() / duration) 
                 << " req/s" << std::endl;
        std::cout << "Max Concurrent Threads: " << max_concurrent_threads.load() << std::endl;
        
        std::cout << "\nRequest Statistics:" << std::endl;
        std::cout << "  Total Sent: " << requests_sent.load() << std::endl;
        std::cout << "  Total Completed: " << results.size() << std::endl;
        std::cout << "  Successful: " << successful << std::endl;
        std::cout << "  Failed: " << failed << std::endl;
        std::cout << "  Success Rate: " << std::fixed << std::setprecision(2)
                 << (successful * 100.0 / results.size()) << "%" << std::endl;
        
        if (failed > 0) {
            std::cout << "\nError Breakdown:" << std::endl;
            for (const auto& [error, count] : error_counts) {
                std::cout << "  " << error << ": " << count << std::endl;
            }
        }
        
        std::cout << "\nThroughput:" << std::endl;
        std::cout << "  Offered Load: " << target_rate << " req/s" << std::endl;
        std::cout << "  Successful Throughput: " << (successful / duration) 
                 << " req/s" << std::endl;
        
        if (successful > 0) {
            double utilization = (successful / duration) / target_rate * 100;
            std::cout << "  Utilization: " << utilization << "%" << std::endl;
        }
        
        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            
            double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
            double mean = sum / latencies.size();
            double median = latencies[latencies.size() / 2];
            double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
            double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
            
            std::cout << "\nLatency Statistics (successful requests, ms):" << std::endl;
            std::cout << "  Mean:   " << mean << std::endl;
            std::cout << "  Median: " << median << std::endl;
            std::cout << "  Min:    " << latencies.front() << std::endl;
            std::cout << "  Max:    " << latencies.back() << std::endl;
            std::cout << "  p95:    " << p95 << std::endl;
            std::cout << "  p99:    " << p99 << std::endl;
        }
        
        save_detailed_results("load_test_detailed.csv");
    }
};

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cout << "Usage: " << argv[0] << " <master_ip> <master_port> <target_rate> <duration_sec> [distribution]" << std::endl;
        std::cout << "Distribution: 'constant' (default) or 'poisson'" << std::endl;
        std::cout << "Example: " << argv[0] << " localhost 5050 100 60 constant" << std::endl;
        std::cout << "\nIMPORTANT: Set ulimit before running!" << std::endl;
        std::cout << "  ulimit -u 65535  # Max user processes" << std::endl;
        std::cout << "  ulimit -n 65535  # Max open files" << std::endl;
        return 1;
    }

    std::string master_ip = argv[1];
    int master_port = std::stoi(argv[2]);
    double target_rate = std::stod(argv[3]);
    int duration = std::stoi(argv[4]);
    std::string distribution = (argc > 5) ? argv[5] : "constant";

    MasterOpenLoopTest tester(master_ip, master_port, target_rate, duration, distribution);
    tester.run_test();

    return 0;
}