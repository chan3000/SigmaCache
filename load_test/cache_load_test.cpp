/////////////////////////////////
// cache_closed_loop_test.cpp
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
#include <map>
#include "client.h"

using namespace std;

enum class OperationType {
    GET,
    POST,
    PUT,
    DELETE
};

struct TestResult {
    OperationType op_type;
    bool success;
    double latency_ms;
    chrono::steady_clock::time_point timestamp;
    string error_msg;
    int client_id;
    int key;
};

// CRITICAL FIX: Share cache server info instead of each thread connecting to master
struct SharedCacheInfo {
    vector<cache_metadata> cache_servers;
    mutex info_mutex;
    
    void set_servers(const cache_metadata* servers, size_t count) {
        lock_guard<mutex> lock(info_mutex);
        cache_servers.clear();
        for (size_t i = 0; i < count; ++i) {
            cache_servers.push_back(servers[i]);
        }
    }
    
    int get_cache_server(int key) {
        lock_guard<mutex> lock(info_mutex);
        for (size_t i = 0; i < cache_servers.size(); ++i) {
            if (key >= cache_servers[i].start_key && key <= cache_servers[i].end_key) {
                return i;
            }
        }
        return -1;
    }
    
    cache_metadata get_server(int index) {
        lock_guard<mutex> lock(info_mutex);
        return cache_servers[index];
    }
    
    size_t num_servers() {
        lock_guard<mutex> lock(info_mutex);
        return cache_servers.size();
    }
};

class CacheClosedLoopTest {
private:
    string master_ip;
    int master_port;
    int num_clients;
    int duration_sec;
    string workload_type;
    
    vector<int> keys;
    atomic<bool> stop_flag{false};
    atomic<int> key_counter{0};
    
    vector<TestResult> results;
    mutex results_mutex;
    
    random_device rd;
    mt19937 gen{rd()};
    
    // Workload parameters
    int cache_size;
    double read_ratio;
    double write_ratio;
    double delete_ratio;
    
    int key_range_start;
    int key_range_end;
    
    // CRITICAL FIX: Shared cache server information
    SharedCacheInfo shared_cache_info;

    void setup_workload(const cache_metadata* servers, size_t num_servers) {
        if (servers && num_servers > 0) {
            key_range_start = servers[0].start_key;
            key_range_end = servers[0].end_key;
            
            // Store for all threads
            shared_cache_info.set_servers(servers, num_servers);
        } else {
            cerr << "Failed to get cache server info" << endl;
            return;
        }
        
        cout << "\nSetting up workload: " << workload_type << endl;
        cout << "Key range: " << key_range_start << "-" << key_range_end << endl;
        
        if (workload_type == "get_popular") {
            cache_size = 100;
            read_ratio = 1.0;
            write_ratio = 0.0;
            delete_ratio = 0.0;
            
            // for (int i = 0; i < cache_size && (key_range_start + i) <= key_range_end; ++i) {
            //     keys.push_back(key_range_start + i);
            // }
            keys.push_back(5);
            keys.push_back(6);
            keys.push_back(7);
            keys.push_back(8);
            keys.push_back(9);
            cout << "Workload: 100% reads on " << keys.size() << " hot keys (cache hits)" << endl;
            
        } else if (workload_type == "get_all") {
            int range = min(5000, key_range_end - key_range_start + 1);
            cache_size = range;
            read_ratio = 1.0;
            write_ratio = 0.0;
            delete_ratio = 0.0;
            
            for (int i = 0; i < range; ++i) {
                keys.push_back(key_range_start + i);
            }
            cout << "Workload: 100% reads on " << keys.size() << " keys (cache misses, DB reads)" << endl;
            
        } else if (workload_type == "put_all") {
            read_ratio = 0.0;
            write_ratio = 1.0;
            delete_ratio = 0.0;
            key_counter.store(key_range_start);
            cout << "Workload: 100% writes (DB writes)" << endl;
            
        } else if (workload_type == "mixed_read_heavy") {
            cache_size = 500;
            read_ratio = 0.80;
            write_ratio = 0.18;
            delete_ratio = 0.02;
            
            int range = min(cache_size, key_range_end - key_range_start + 1);
            for (int i = 0; i < range; ++i) {
                keys.push_back(key_range_start + i);
            }
            cout << "Workload: 80% reads, 18% writes, 2% deletes on " << keys.size() << " keys" << endl;
            
        } else if (workload_type == "mixed_write_heavy") {
            cache_size = 500;
            read_ratio = 0.20;
            write_ratio = 0.70;
            delete_ratio = 0.10;
            
            int range = min(cache_size, key_range_end - key_range_start + 1);
            for (int i = 0; i < range; ++i) {
                keys.push_back(key_range_start + i);
            }
            cout << "Workload: 20% reads, 70% writes, 10% deletes on " << keys.size() << " keys" << endl;
            
        } else if (workload_type == "mixed_balanced") {
            cache_size = 500;
            read_ratio = 0.50;
            write_ratio = 0.40;
            delete_ratio = 0.10;
            
            int range = min(cache_size, key_range_end - key_range_start + 1);
            for (int i = 0; i < range; ++i) {
                keys.push_back(key_range_start + i);
            }
            cout << "Workload: 50% reads, 40% writes, 10% deletes on " << keys.size() << " keys" << endl;
            
        } else if (workload_type == "zipfian") {
            cache_size = 1000;
            read_ratio = 0.95;
            write_ratio = 0.04;
            delete_ratio = 0.01;
            
            int range = min(cache_size, key_range_end - key_range_start + 1);
            for (int i = 0; i < range; ++i) {
                keys.push_back(key_range_start + i);
            }
            cout << "Workload: Zipfian (95% reads, 4% writes, 1% deletes) on " << keys.size() << " keys" << endl;
        }
    }

    void pre_populate(Client& setup_client) {
        if (workload_type == "put_all") return;
        
        cout << "Pre-populating with " << keys.size() << " keys..." << endl;
        uniform_int_distribution<> val_dis(1, 10000);
        
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i % 100 == 0) {
                cout << "  " << i << "/" << keys.size() << "\r" << flush;
            }
            int value = val_dis(gen);
            setup_client.send_request(request_t::POST, keys[i], value, false);
        }
        cout << "  " << keys.size() << "/" << keys.size() << endl;
        cout << "Pre-population complete!" << endl;
    }

    int get_zipfian_key(mt19937& thread_gen) {
        uniform_real_distribution<> dis(0.0, 1.0);
        double r = dis(thread_gen);
        
        int index;
        if (r < 0.8) {
            uniform_int_distribution<> hot_dis(0, keys.size() / 5);
            index = hot_dis(thread_gen);
        } else {
            uniform_int_distribution<> cold_dis(keys.size() / 5, keys.size() - 1);
            index = cold_dis(thread_gen);
        }
        
        return keys[min(index, (int)keys.size() - 1)];
    }

    // CRITICAL FIX: Direct cache server communication without going through master
    TestResult execute_operation_direct(httplib::Client& cache_client, int client_id, 
                                       mt19937& thread_gen, int key, int value, 
                                       request_t::request_t req_type, OperationType op_type) {
        auto start = chrono::steady_clock::now();
        
        try {
            httplib::Result res;
            
            switch(req_type) {
                case request_t::GET: {
                    res = cache_client.Get(("/kv_cache/" + to_string(key)).c_str());
                    break;
                }
                case request_t::POST: {
                    //cout << "Sending request key = " << key << endl;
                    res = cache_client.Post(("/kv_cache/" + to_string(key) + "/" + to_string(value)).c_str());
                    break;
                }
                case request_t::PUT: {
                    //cout << "Sending request key = " << key << endl;
                    res = cache_client.Put(("/kv_cache/" + to_string(key) + "/" + to_string(value)).c_str());
                    break;
                }
                case request_t::DELETE: {
                    res = cache_client.Delete(("/kv_cache/" + to_string(key)).c_str());
                    break;
                }
            }
            
            auto end = chrono::steady_clock::now();
            double latency = chrono::duration<double, milli>(end - start).count();
            
            bool success = (res && (res->status == 200 || res->status == 201));
            string error_msg = success ? "" : (res ? "HTTP " + to_string(res->status) : "Connection failed");
            
            return {op_type, success, latency, end, error_msg, client_id, key};
            
        } catch (const exception& e) {
            auto end = chrono::steady_clock::now();
            double latency = chrono::duration<double, milli>(end - start).count();
            return {op_type, false, latency, end, string("Exception: ") + e.what(), client_id, key};
        }
    }

    TestResult execute_operation(httplib::Client& cache_client, int client_id, mt19937& thread_gen) {
        OperationType op_type;
        int key;
        int value;
        request_t::request_t req_type;
        
        uniform_real_distribution<> dis(0.0, 1.0);
        uniform_int_distribution<> val_dis(1, 10000);
        double rand = dis(thread_gen);
        
        if (rand < read_ratio) {
            op_type = OperationType::GET;
            req_type = request_t::GET;
            
            if (workload_type == "zipfian") {
                key = get_zipfian_key(thread_gen);
            } else if (workload_type == "put_all") {
                key = key_range_start;
            } else {
                uniform_int_distribution<> key_dis(0, keys.size() - 1);
                key = keys[key_dis(thread_gen)];
            }
            value = -1;
            
        } else if (rand < read_ratio + write_ratio) {
            op_type = OperationType::PUT;
            req_type = request_t::PUT;
            value = val_dis(thread_gen);
            
            if (workload_type == "put_all") {
                int counter = key_counter.fetch_add(1);
                if (counter > key_range_end) {
                    key_counter.store(key_range_start);
                    counter = key_range_start;
                }
                key = counter;
            } else if (workload_type == "zipfian") {
                key = get_zipfian_key(thread_gen);
            } else {
                uniform_int_distribution<> key_dis(0, keys.size() - 1);
                key = keys[key_dis(thread_gen)];
            }
            
        } else {
            op_type = OperationType::DELETE;
            req_type = request_t::DELETE;
            value = -1;
            
            if (workload_type == "zipfian") {
                key = get_zipfian_key(thread_gen);
            } else {
                uniform_int_distribution<> key_dis(0, keys.size() - 1);
                key = keys[key_dis(thread_gen)];
            }
        }
        
        return execute_operation_direct(cache_client, client_id, thread_gen, key, value, req_type, op_type);
    }

    // CRITICAL FIX: Each thread connects directly to cache server, not master
    void client_worker(int client_id) {
        // Get cache server info (already fetched once)
        int server_index = 0; // For simplicity, using first server
        cache_metadata server_info = shared_cache_info.get_server(server_index);
        
        // Connect directly to cache server
        httplib::Client cache_client(server_info.ip_address, server_info.port);
        cache_client.set_connection_timeout(600, 0);
        cache_client.set_read_timeout(600, 0);
        cache_client.set_keep_alive(true);
        
        mt19937 thread_gen(rd() + client_id);
        
        vector<TestResult> local_results{};
        //local_results.reserve(100000);
        
        //cout << "[Client " << client_id << "] Started (direct connection to cache)" << endl;
        
        while (!stop_flag.load()) {
            auto result = execute_operation(cache_client, client_id, thread_gen);
            local_results.push_back(result);
        }
        
        //cout << "[Client " << client_id << "] Completed " << local_results.size() << " requests" << endl;
        
        lock_guard<mutex> lock(results_mutex);
        results.insert(results.end(), local_results.begin(), local_results.end());
    }

public:
    CacheClosedLoopTest(const string& master_ip_param, int master_port_param, 
                       int clients, int duration, const string& workload)
        : master_ip(master_ip_param), master_port(master_port_param),
          num_clients(clients), duration_sec(duration), workload_type(workload) {
    }

    void run_test() {
        // CRITICAL FIX: Only ONE connection to master server at startup
        cout << "Connecting to master server to get cache server info..." << endl;
        Client setup_client(master_ip, master_port);
        
        if (!setup_client.initiate_connection()) {
            cerr << "Failed to connect to master server!" << endl;
            return;
        }
        
        cout << "Successfully retrieved cache server information" << endl;
        
        setup_workload(setup_client.get_cache_servers(), setup_client.get_num_servers());
        
        // Pre-populate if needed
        //pre_populate(setup_client);
        
        cout << "\n=== Starting Closed-Loop Cache Server Load Test ===" << endl;
        cout << "Workload: " << workload_type << endl;
        cout << "Concurrent Clients (Threads): " << num_clients << endl;
        cout << "Duration: " << duration_sec << "s" << endl;
        cout << "\nEach client connects directly to cache server (not through master)" << endl;
        cout << "Each client maintains a persistent connection" << endl;
        cout << endl;
        
        vector<thread> threads;
        threads.reserve(num_clients);
        
        auto start_time = chrono::steady_clock::now();
        
        // Start all client threads - they connect directly to cache server
        cout << "Starting " << num_clients << " client threads..." << endl;
        for (int i = 0; i < num_clients; ++i) {
            threads.emplace_back(&CacheClosedLoopTest::client_worker, this, i);
            
            // Stagger thread creation slightly to avoid thundering herd
            if (i % 10 == 9) {
                this_thread::sleep_for(chrono::milliseconds(10));
            }
        }
        
        // Monitor progress
        for (int i = 0; i < duration_sec; ++i) {
            this_thread::sleep_for(chrono::seconds(1));
            int current_count = results.size();
            cout << "Time: " << (i+1) << "s | Requests: " << current_count 
                 << " | Rate: " << (current_count / (i+1)) << " req/s\r" << flush;
        }
        cout << endl;
        
        stop_flag.store(true);
        
        cout << "\nWaiting for all client threads to finish..." << endl;
        for (auto& t : threads) {
            t.join();
        }
        
        auto end_time = chrono::steady_clock::now();
        double actual_duration = chrono::duration<double>(end_time - start_time).count();
        
        print_results(actual_duration, start_time);
    }

    void save_detailed_results(const string& filename, chrono::steady_clock::time_point test_start) {
        ofstream file(filename);
        file << "timestamp_ms,client_id,op_type,key,latency_ms,success,error\n";
        
        for (const auto& result : results) {
            double timestamp = chrono::duration<double, milli>(result.timestamp - test_start).count();
            file << fixed << setprecision(3)
                 << timestamp << ","
                 << result.client_id << ","
                 << op_type_to_string(result.op_type) << ","
                 << result.key << ","
                 << result.latency_ms << ","
                 << (result.success ? 1 : 0) << ","
                 << result.error_msg << "\n";
        }
        
        file.close();
        cout << "Detailed results saved to: " << filename << endl;
    }

private:
    string op_type_to_string(OperationType op) {
        switch (op) {
            case OperationType::GET: return "GET";
            case OperationType::POST: return "POST";
            case OperationType::PUT: return "PUT";
            case OperationType::DELETE: return "DELETE";
            default: return "UNKNOWN";
        }
    }

    void print_results(double duration, chrono::steady_clock::time_point test_start) {
        if (results.empty()) {
            cout << "No results collected!" << endl;
            return;
        }
        
        int successful = 0;
        int failed = 0;
        vector<double> latencies;
        map<OperationType, int> op_counts;
        map<OperationType, int> op_success_counts;
        map<OperationType, vector<double>> op_latencies;
        map<string, int> error_counts;
        map<int, int> client_request_counts;
        
        for (const auto& result : results) {
            client_request_counts[result.client_id]++;
            
            if (result.success) {
                successful++;
                latencies.push_back(result.latency_ms);
                op_success_counts[result.op_type]++;
                op_latencies[result.op_type].push_back(result.latency_ms);
            } else {
                failed++;
                error_counts[result.error_msg]++;
            }
            op_counts[result.op_type]++;
        }
        
        cout << "\n=== Closed-Loop Cache Server Load Test Results ===" << endl;
        cout << "Workload Type: " << workload_type << endl;
        cout << "Duration: " << fixed << setprecision(2) << duration << "s" << endl;
        cout << "Concurrent Clients: " << num_clients << endl;
        
        cout << "\nRequest Statistics:" << endl;
        cout << "  Total Requests: " << results.size() << endl;
        cout << "  Successful: " << successful << endl;
        cout << "  Failed: " << failed << endl;
        cout << "  Success Rate: " << fixed << setprecision(2) 
             << (successful * 100.0 / results.size()) << "%" << endl;
        
        if (failed > 0) {
            cout << "\nError Breakdown:" << endl;
            for (const auto& [error, count] : error_counts) {
                cout << "  " << error << ": " << count << endl;
            }
        }
        
        cout << "\nThroughput:" << endl;
        cout << "  Successful Throughput: " << fixed << setprecision(2) 
             << (successful / duration) << " req/s" << endl;
        cout << "  Per-Client Throughput: " << fixed << setprecision(2) 
             << (successful / duration / num_clients) << " req/s/client" << endl;
        
        if (!latencies.empty()) {
            sort(latencies.begin(), latencies.end());
            
            double sum = accumulate(latencies.begin(), latencies.end(), 0.0);
            double mean = sum / latencies.size();
            double median = latencies[latencies.size() / 2];
            double p95 = latencies[static_cast<size_t>(latencies.size() * 0.95)];
            double p99 = latencies[static_cast<size_t>(latencies.size() * 0.99)];
            
            cout << "\nOverall Latency Statistics (successful requests, ms):" << endl;
            cout << "  Mean:   " << fixed << setprecision(2) << mean << endl;
            cout << "  Median: " << median << endl;
            cout << "  Min:    " << latencies.front() << endl;
            cout << "  Max:    " << latencies.back() << endl;
            cout << "  p95:    " << p95 << endl;
            cout << "  p99:    " << p99 << endl;
        }
        
        cout << "\nOperation Breakdown:" << endl;
        for (const auto& [op, count] : op_counts) {
            int success_count = op_success_counts[op];
            cout << "  " << op_type_to_string(op) << ": " 
                 << count << " total (" 
                 << fixed << setprecision(1) << (count * 100.0 / results.size()) << "%), "
                 << success_count << " successful ("
                 << fixed << setprecision(1) << (success_count * 100.0 / count) << "% success rate)";
            
            if (op_latencies[op].size() > 0) {
                auto& op_lat = op_latencies[op];
                sort(op_lat.begin(), op_lat.end());
                double op_mean = accumulate(op_lat.begin(), op_lat.end(), 0.0) / op_lat.size();
                double op_p95 = op_lat[static_cast<size_t>(op_lat.size() * 0.95)];
                cout << " [avg: " << fixed << setprecision(2) << op_mean 
                     << "ms, p95: " << op_p95 << "ms]";
            }
            cout << endl;
        }
        
        save_detailed_results("load_test_detailed.csv", test_start);
    }
};

int main(int argc, char* argv[]) {
    if (argc < 6) {
        cout << "Usage: " << argv[0] << " <master_ip> <master_port> <num_clients> <duration_sec> <workload>" << endl;
        cout << "\nAvailable Workloads:" << endl;
        cout << "  get_popular        - 100% reads on hot keys (CPU bound, cache hits)" << endl;
        cout << "  get_all            - 100% reads on large dataset (I/O bound, cache misses)" << endl;
        cout << "  put_all            - 100% writes (I/O bound, database writes)" << endl;
        cout << "  mixed_read_heavy   - 80% reads, 18% writes, 2% deletes" << endl;
        cout << "  mixed_write_heavy  - 20% reads, 70% writes, 10% deletes" << endl;
        cout << "  mixed_balanced     - 50% reads, 40% writes, 10% deletes" << endl;
        cout << "  zipfian            - 95% reads with Zipfian distribution (realistic)" << endl;
        cout << "\nNote: Connects to master ONCE, then all clients connect directly to cache server" << endl;
        cout << "Example: " << argv[0] << " localhost 5050 10 300 get_popular" << endl;
        return 1;
    }

    string master_ip = argv[1];
    int master_port = stoi(argv[2]);
    int num_clients = stoi(argv[3]);
    int duration = stoi(argv[4]);
    string workload = argv[5];

    CacheClosedLoopTest tester(master_ip, master_port, num_clients, duration, workload);
    tester.run_test();

    return 0;
}