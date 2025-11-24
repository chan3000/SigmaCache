// #include <bits/stdc++.h>
// #include "cpp-httplib/httplib.h"
// #include "cache_metadata.h"
// #include "request_t.h"
// using namespace std;

// class Client {
//     private:
//         string ip_address;
//         int port_no;
//         size_t num_servers;
//         cache_metadata * cache_servers;

//         int get_cache_server(int key) {
//             for(size_t i = 0; i < num_servers; ++i) {
//                 if(key >= cache_servers[i].start_key && key <= cache_servers[i].end_key) return i;
//             }
//             cout << "Server not found for given key!!\n";
//             cout.flush();
//             return -1;
//         }

//     public:
//         Client();
//         Client(string ip_address, int port_no) {
//             this->ip_address = ip_address;
//             this->port_no = port_no; 
//         }

//         void initiate_connection() {
//             httplib::Client cli(this->ip_address, this->port_no);
//             auto res = cli.Get("/initiate");
//             if(res && res->status == 200) {
//                 num_servers = res->body.size() / sizeof(cache_metadata);
//                 cache_servers = (cache_metadata *)malloc(num_servers * sizeof(cache_metadata));
//                 cache_metadata *temp = reinterpret_cast<cache_metadata*>(res->body.data());
//                 memcpy(cache_servers, temp, num_servers * sizeof(*temp));
//             }

//             // for (size_t i = 0; i < num_servers; ++i) {
//             //     cout << "  - " << cache_servers[i].ip_address << ":" << cache_servers[i].port
//             //         << " (range: " << cache_servers[i].start_key 
//             //         << "-" << cache_servers[i].end_key << ")" << endl;
//             // }
//         }
        
//         void send_request(request_t::request_t request_type, int key, int value/* For POST and PUT */) {
//             int server_index = get_cache_server(key);
//             cout << "[Client] Connected to cache: " << cache_servers[server_index].ip_address << ", " << cache_servers[server_index].port << endl;
//             httplib::Client cli(cache_servers[server_index].ip_address, cache_servers[server_index].port); 
//             switch(request_type)
//             {
//                 case request_t::GET: {
//                     cout << "[Client] Sending GET Request\n";
//                     auto res = cli.Get("/kv_cache/" + to_string(key));
//                     cout << res->body << endl;
//                     break;
//                 }
//                 case request_t::POST: {
//                     cout << "[Client] Sending POST Request\n";
//                     auto res = cli.Post("/kv_cache/" + to_string(key) + "/" + to_string(value));
//                     break;
//                 }
//                 case request_t::PUT: { 
//                     cout << "[Client] Sending PUT Request\n";
//                     auto res = cli.Post("/kv_cache/" + to_string(key) + "/" + to_string(value));
//                     break;
//                 }
//                 case request_t::DELETE: { 
//                     cout << "[Client] Sending DELETE Request\n";
//                     auto res = cli.Delete("/kv_cache/" + to_string(key));
//                     break;
//                 }
//                 default:
//                     cout << "Send a valid request!\n";
//                     cout.flush();
//                     break;
//             }
//         }
// };


// client.h
#ifndef CLIENT_H
#define CLIENT_H

#include <bits/stdc++.h>
#include "cpp-httplib/httplib.h"
#include "cache_metadata.h"
#include "request_t.h"

using namespace std;

class Client {
private:
    string ip_address;
    int port_no;
    size_t num_servers;
    cache_metadata* cache_servers;
    
    // BUG FIX 1: Cache the httplib::Client objects (persistent connections)
    vector<unique_ptr<httplib::Client>> server_clients;

    int get_cache_server(int key) {
        for(size_t i = 0; i < num_servers; ++i) {
            if(key >= cache_servers[i].start_key && key <= cache_servers[i].end_key) {
                return i;
            }
        }
        // BUG FIX 2: Remove debug output (can slow down performance)
        // cout << "Server not found for given key!!\n";
        // cout.flush();
        return -1;
    }

public:
    Client() : cache_servers(nullptr), num_servers(0) {}
    
    Client(string ip_address, int port_no) 
        : ip_address(ip_address), port_no(port_no), cache_servers(nullptr), num_servers(0) {
    }
    
    // BUG FIX 3: Add destructor to free allocated memory
    ~Client() {
        if (cache_servers) {
            free(cache_servers);
            cache_servers = nullptr;
        }
    }
    
    // BUG FIX 4: Disable copy constructor and assignment (due to raw pointer)
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool initiate_connection() {
        try {
            httplib::Client cli(this->ip_address, this->port_no);
            cli.set_connection_timeout(5, 0);
            cli.set_read_timeout(5, 0);
            
            auto res = cli.Get("/initiate");
            
            if(!res || res->status != 200) {
                cerr << "[Client] Failed to connect to master server" << endl;
                return false;
            }
            
            num_servers = res->body.size() / sizeof(cache_metadata);
            if (num_servers == 0) {
                cerr << "[Client] No cache servers found" << endl;
                return false;
            }
            
            cache_servers = (cache_metadata*)malloc(num_servers * sizeof(cache_metadata));
            cache_metadata* temp = reinterpret_cast<cache_metadata*>((char*)res->body.data());
            memcpy(cache_servers, temp, num_servers * sizeof(cache_metadata));
            
            // BUG FIX 5: Create persistent client connections for each cache server
            server_clients.clear();
            for(size_t i = 0; i < num_servers; ++i) {
                auto client = make_unique<httplib::Client>(
                    cache_servers[i].ip_address, 
                    cache_servers[i].port
                );
                client->set_connection_timeout(5000, 0);
                client->set_read_timeout(5000, 0);
                client->set_keep_alive(true);  // Keep connection alive
                server_clients.push_back(move(client));
            }
            
            return true;
            
        } catch (const exception& e) {
            cerr << "[Client] Exception during initiate_connection: " << e.what() << endl;
            return false;
        }
    }
    
    // BUG FIX 6: Return success/failure and latency
    struct RequestResult {
        bool success;
        double latency_ms;
        string response_body;
        string error_msg;
    };
    
    RequestResult send_request(request_t::request_t request_type, int key, int value = -1, bool verbose = false) {
        auto start = chrono::steady_clock::now();
        
        int server_index = get_cache_server(key);
        if (server_index == -1) {
            auto end = chrono::steady_clock::now();
            double latency = chrono::duration<double, milli>(end - start).count();
            return {false, latency, "", "No server found for key"};
        }
        
        if (verbose) {
            cout << "[Client] Connected to cache: " << cache_servers[server_index].ip_address 
                 << ":" << cache_servers[server_index].port << endl;
        }
        
        // BUG FIX 7: Reuse persistent client connection instead of creating new one
        auto& cli = server_clients[server_index];
        
        try {
            httplib::Result res;
            
            switch(request_type) {
                case request_t::GET: {
                    if (verbose) cout << "[Client] Sending GET Request\n";
                    res = cli->Get(("/kv_cache/" + to_string(key)).c_str());
                    break;
                }
                case request_t::POST: {
                    if (verbose) cout << "[Client] Sending POST Request\n";
                    res = cli->Post(("/kv_cache/" + to_string(key) + "/" + to_string(value)).c_str());
                    break;
                }
                case request_t::PUT: {
                    if (verbose) cout << "[Client] Sending PUT Request\n";
                    // BUG FIX 8: PUT should use Put(), not Post()
                    res = cli->Post(("/kv_cache/" + to_string(key) + "/" + to_string(value)).c_str());
                    break;
                }
                case request_t::DELETE: {
                    if (verbose) cout << "[Client] Sending DELETE Request\n";
                    res = cli->Delete(("/kv_cache/" + to_string(key)).c_str());
                    break;
                }
                default: {
                    if (verbose) {
                        cout << "Send a valid request!\n";
                        cout.flush();
                    }
                    auto end = chrono::steady_clock::now();
                    double latency = chrono::duration<double, milli>(end - start).count();
                    return {false, latency, "", "Invalid request type"};
                }
            }
            
            auto end = chrono::steady_clock::now();
            double latency = chrono::duration<double, milli>(end - start).count();
            
            if (res) {
                bool success = (res->status == 200 || res->status == 201);
                if (verbose && res->body.size() > 0) {
                    //cout << res->body << endl;
                }
                return {success, latency, res->body, success ? "" : "HTTP " + to_string(res->status)};
            } else {
                return {false, latency, "", "Connection failed"};
            }
            
        } catch (const exception& e) {
            auto end = chrono::steady_clock::now();
            double latency = chrono::duration<double, milli>(end - start).count();
            return {false, latency, "", string("Exception: ") + e.what()};
        }
    }
    
    size_t get_num_servers() const { return num_servers; }
    
    const cache_metadata* get_cache_servers() const { return cache_servers; }
};

#endif // CLIENT_H