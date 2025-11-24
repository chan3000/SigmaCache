// #ifndef CACHE_H
// #define CACHE_H

// #include <bits/stdc++.h>
// #include <shared_mutex>
// #include <atomic>

// #include "cpp-httplib/httplib.h"
// #include <mysql_driver.h>
// #include <mysql_connection.h>
// #include <cppconn/statement.h>
// #include <cppconn/prepared_statement.h>
// #include <cppconn/resultset.h>
// #include <cppconn/exception.h>

// #include "cache_metadata.h"
// #include "request_t.h"
// #include "db_connector.h"

// using namespace std;

// class Cache {
//     private:
//         static const size_t NUM_SHARDS = 16;
//         static const size_t SHARD_SIZE = 100;
        
//         struct ConnectionPool {
//             vector<Database_Connector*> all_connections;
//             queue<Database_Connector*> available_connections;
//             mutex pool_mutex;
//             condition_variable pool_cv;
//             int pool_size;
//             atomic<int> active_connections{0};
//             atomic<int> total_timeouts{0};
//             atomic<int> total_reconnects{0};
//             string host, user, password, database_name, table_name;
            
//             thread keep_alive_thread;
//             atomic<bool> shutdown{false};
            
//             ConnectionPool(int size, string h, string u, string p, string db, string tbl) 
//                 : pool_size(size), host(h), user(u), password(p), 
//                   database_name(db), table_name(tbl) {
                
//                 cout << "[Pool] Creating " << size << " connections..." << endl;
                
//                 for (int i = 0; i < pool_size; ++i) {
//                     try {
//                         auto* conn = new Database_Connector(host, user, password, database_name, table_name);
//                         if (conn->is_alive()) {
//                             all_connections.push_back(conn);
//                             available_connections.push(conn);
//                             if ((i+1) % 10 == 0 || i == pool_size - 1) {
//                                 cout << "[Pool] Created " << (i+1) << "/" << size << endl;
//                             }
//                         } else {
//                             delete conn;
//                         }
//                     } catch (const exception& e) {
//                         cerr << "[Pool] Exception " << (i+1) << endl;
//                     }
//                 }
                
//                 if (all_connections.empty()) {
//                     throw runtime_error("Failed to create any database connections!");
//                 }
                
//                 cout << "[Pool] Created " << all_connections.size() << " connections" << endl;
//                 keep_alive_thread = thread(&ConnectionPool::keep_alive_worker, this);
//             }
            
//             void keep_alive_worker() {
//                 while (!shutdown.load()) {
//                     this_thread::sleep_for(chrono::minutes(5));
                    
//                     if (shutdown.load()) break;
                    
//                     vector<Database_Connector*> to_ping;
//                     {
//                         lock_guard<mutex> lock(pool_mutex);
//                         queue<Database_Connector*> temp = available_connections;
//                         while (!temp.empty()) {
//                             to_ping.push_back(temp.front());
//                             temp.pop();
//                         }
//                     }
                    
//                     int pinged = 0;
//                     for (auto* conn : to_ping) {
//                         if (conn->ping()) pinged++;
//                     }
                    
//                     if (pinged > 0) {
//                         cout << "[Pool] Keep-alive: pinged " << pinged << endl;
//                     }
//                 }
//             }
            
//             Database_Connector* get_connection(int timeout_ms = 3000) {
//                 auto start = chrono::steady_clock::now();
//                 unique_lock<mutex> lock(pool_mutex);
                
//                 auto deadline = start + chrono::milliseconds(timeout_ms);
                
//                 if (!pool_cv.wait_until(lock, deadline, [this] { return !available_connections.empty(); })) {
//                     total_timeouts++;
//                     return nullptr;
//                 }
                
//                 auto* conn = available_connections.front();
//                 available_connections.pop();
//                 active_connections++;
                
//                 lock.unlock();
                
//                 if (!conn->is_alive()) {
//                     total_reconnects++;
//                     conn->ensure_connection();
//                 }
                
//                 return conn;
//             }
            
//             void return_connection(Database_Connector* conn) {
//                 if (!conn) return;
                
//                 lock_guard<mutex> lock(pool_mutex);
//                 available_connections.push(conn);
//                 active_connections--;
//                 pool_cv.notify_one();
//             }
            
//             ~ConnectionPool() {
//                 shutdown.store(true);
//                 if (keep_alive_thread.joinable()) {
//                     keep_alive_thread.join();
//                 }
                
//                 for (auto* conn : all_connections) {
//                     delete conn;
//                 }
//             }
//         };
        
//         ConnectionPool* db_pool;
        
//         struct CacheEntry {
//             int value;
//             bool dirty;
            
//             CacheEntry() : value(0), dirty(false) {}
//             CacheEntry(int v, bool d) : value(v), dirty(d) {}
//         };
        
//         struct Shard {
//             unordered_map<int, CacheEntry> cache_lines;
//             shared_mutex shard_lock;

//             queue<pair<int, int>> *write_back_queue;
//             condition_variable *cv_write_back;
//             mutex *write_back_lock;
            
//             typedef struct key_node {
//                 int key;
//                 struct key_node *previous;
//                 struct key_node *next;
//             } key_node;

//             key_node *head, *tail;
//             map<int, key_node *> access_list;
            
//             ~Shard() {
//                 while (head != NULL) {
//                     key_node* temp = head;
//                     head = head->next;
//                     delete temp;
//                 }
//                 access_list.clear();
//             }

//             void remove_from_access_list(int key) {
//                 if(access_list.find(key) == access_list.end()) return;
                
//                 key_node *current = access_list[key];
//                 key_node *previous = current->previous;
//                 key_node* next = current->next;
                
//                 access_list.erase(key);

//                 if(previous != NULL) previous->next = next;
//                 if(next != NULL) next->previous = previous;

//                 if(tail == current) tail = previous;
//                 if(head == current) head = next;

//                 current->previous = NULL;
//                 current->next = NULL;
                
//                 delete current;
//             }

//             void evict_from_shard(int key, bool update_access_list) {
//                 if(update_access_list) remove_from_access_list(key);
//                 if(cache_lines.find(key) == cache_lines.end()) return;
                
//                 // Only write back if dirty
//                 if (cache_lines[key].dirty) {
//                     std::lock_guard<std::mutex> lk(*write_back_lock);
//                     (*write_back_queue).push(make_pair(key, cache_lines[key].value));
//                     (*cv_write_back).notify_one();
//                 }
                
//                 cache_lines.erase(key);
//             }

//             void lru() {
//                 if (tail == NULL) return;
//                 int key = tail->key;
//                 evict_from_shard(key, true);
//             }

//             void move_to_top(int key) {
//                 if(head != NULL && head->key == key) return;
                
//                 key_node *current = nullptr;
                
//                 if(access_list.find(key) != access_list.end()) {
//                     current = access_list[key];
                    
//                     key_node *previous = current->previous;
//                     key_node *next = current->next;
                    
//                     if(previous == NULL) return;
                    
//                     previous->next = next;
//                     if(next != NULL) next->previous = previous;
//                     if(current == tail) tail = previous;
                    
//                     current->next = head;
//                     current->previous = NULL;
//                     if(head != NULL) head->previous = current;
//                     head = current;
                    
//                 } else {
//                     current = new key_node;
//                     current->key = key;
//                     current->previous = NULL;
//                     current->next = NULL;
//                     access_list[key] = current;
                    
//                     if(head == NULL) {
//                         head = current;
//                         tail = current;
//                     } else {
//                         current->next = head;
//                         head->previous = current;
//                         head = current;
//                     }
//                 }
//             }

//             void add_to_shard(int key, int value, bool dirty) {
//                 // If key already exists, just update it
//                 if(cache_lines.find(key) != cache_lines.end()) {
//                     cache_lines[key].value = value;
//                     cache_lines[key].dirty = dirty;
//                     move_to_top(key);
//                     return;
//                 }
                
//                 // Evict if full
//                 if(cache_lines.size() >= SHARD_SIZE) {
//                     lru();
//                 }
                
//                 cache_lines[key] = CacheEntry(value, dirty);
//                 move_to_top(key);
//             }
            
//             void update_value(int key, int value, bool dirty) {
//                 if(cache_lines.find(key) != cache_lines.end()) {
//                     cache_lines[key].value = value;
//                     cache_lines[key].dirty = dirty;
//                     move_to_top(key);
//                 } else {
//                     add_to_shard(key, value, dirty);
//                 }
//             }
//         };

//         Shard cache_shards[NUM_SHARDS];
//         queue<pair<int, int>> write_back_queue;
//         condition_variable cv_write_back;
//         mutex write_back_lock;
//         thread write_back_thread;
//         atomic<bool> shutdown_flag{false};
        
//         string host, user, password, database_name, table_name;
        
//         atomic<uint64_t> cache_hits{0};
//         atomic<uint64_t> cache_misses{0};
//         atomic<uint64_t> total_reads{0};
//         atomic<uint64_t> total_writes{0};
//         atomic<uint64_t> write_backs_queued{0};

//         int get_shard_mapping(int key) {
//             return (key % NUM_SHARDS);
//         }

//         void delayed_write_to_database() {
//             while(!shutdown_flag.load()) {
//                 vector<pair<int, int>> batch;
                
//                 {
//                     std::unique_lock<std::mutex> lk(write_back_lock);
                    
//                     cv_write_back.wait_for(lk, chrono::milliseconds(100), 
//                         [&]{ return !write_back_queue.empty() || shutdown_flag.load(); });
                    
//                     if (shutdown_flag.load() && write_back_queue.empty()) {
//                         break;
//                     }
                    
//                     int batch_size = min(100, (int)write_back_queue.size());
//                     for (int i = 0; i < batch_size && !write_back_queue.empty(); ++i) {
//                         batch.push_back(write_back_queue.front());
//                         write_back_queue.pop();
//                     }
//                 }
                
//                 if (batch.empty()) {
//                     continue;
//                 }
                
//                 Database_Connector* database = db_pool->get_connection(2000);
//                 if (!database) {
//                     {
//                         lock_guard<mutex> lk(write_back_lock);
//                         for (const auto& item : batch) {
//                             write_back_queue.push(item);
//                         }
//                     }
//                     this_thread::sleep_for(chrono::milliseconds(100));
//                     continue;
//                 }
                
//                 for (const auto& kv : batch) {
//                     try {
//                         database->run_query(kv.first, kv.second, request_t::POST);
//                     } catch (const exception& e) {
//                         cerr << "[WriteBack] Error: " << e.what() << endl;
//                     }
//                 }
                
//                 db_pool->return_connection(database);
                
//                 int queue_size;
//                 {
//                     lock_guard<mutex> lk(write_back_lock);
//                     queue_size = write_back_queue.size();
//                 }
                
//                 if (queue_size > 1000) {
//                     this_thread::sleep_for(chrono::milliseconds(1));
//                 } else {
//                     this_thread::sleep_for(chrono::milliseconds(10));
//                 }
//             }
//         }

//         void start_server(string ip_address, int port_no) {
//             httplib::Server svr;
            
//             svr.set_keep_alive_max_count(100);
//             svr.set_keep_alive_timeout(5);
            
//             svr.Get("/stats", [&](const httplib::Request& req, httplib::Response& res) {
//                 int queue_size;
//                 {
//                     lock_guard<mutex> lk(write_back_lock);
//                     queue_size = write_back_queue.size();
//                 }
                
//                 stringstream ss;
//                 ss << "=== Cache Stats (Async Write-Back) ===\n";
//                 ss << "Cache Hits: " << cache_hits.load() << "\n";
//                 ss << "Cache Misses: " << cache_misses.load() << "\n";
//                 ss << "Total Reads: " << total_reads.load() << "\n";
//                 ss << "Total Writes: " << total_writes.load() << "\n";
//                 ss << "Write-backs Queued: " << write_backs_queued.load() << "\n";
                
//                 uint64_t total = cache_hits.load() + cache_misses.load();
//                 if (total > 0) {
//                     ss << "Hit Rate: " << fixed << setprecision(2) 
//                        << (cache_hits.load() * 100.0 / total) << "%\n";
//                 }
                
//                 ss << "\n=== Pool Stats ===\n";
//                 {
//                     lock_guard<mutex> lock(db_pool->pool_mutex);
//                     ss << "Available: " << db_pool->available_connections.size() << "\n";
//                     ss << "Active: " << db_pool->active_connections.load() << "\n";
//                     ss << "Timeouts: " << db_pool->total_timeouts.load() << "\n";
//                 }
                
//                 ss << "\n=== Write-back Queue ===\n";
//                 ss << "Current Size: " << queue_size << "\n";
                
//                 res.set_content(ss.str(), "text/plain");
//             });

//             // GET: Reads can be slow if using unique_lock
//             svr.Get("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
//                 try {
//                     int key = stoi(req.path_params.at("key"));
//                     int shard_index = get_shard_mapping(key);
//                     auto& current_shard = cache_shards[shard_index];
//                     auto& cache_lines = current_shard.cache_lines;

//                     total_reads++;
//                     int value = -1;
//                     bool cache_hit = false;
                    
//                     // BOTTLENECK: Using unique_lock for ALL reads
//                     // This serializes all GET requests to the same shard!
//                     {
//                         unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
//                         auto it = cache_lines.find(key);
//                         if(it != cache_lines.end()) {
//                             value = it->second.value;
//                             cache_hit = true;
//                             // Update LRU while holding lock
//                             current_shard.move_to_top(key);
//                         }
//                     }
                    
//                     if (cache_hit) {
//                         cache_hits++;
//                         res.set_content(to_string(value), "text/plain");
//                         return;
//                     }
                    
//                     // Cache miss
//                     cache_misses++;
                    
//                     Database_Connector* database = nullptr;
//                     for (int attempt = 0; attempt < 3; ++attempt) {
//                         database = db_pool->get_connection(1000);
//                         if (database) break;
//                         this_thread::sleep_for(chrono::milliseconds(10));
//                     }
                    
//                     if (!database) {
//                         res.status = 503;
//                         res.set_content("Service unavailable", "text/plain");
//                         return;
//                     }
                    
//                     try {
//                         database->run_query(key, -1, request_t::GET);
//                         value = database->current_value;
//                     } catch (const exception& e) {
//                         db_pool->return_connection(database);
//                         res.status = 500;
//                         return;
//                     }
                    
//                     db_pool->return_connection(database);
                    
//                     // Add to cache as CLEAN
//                     {
//                         unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
//                         current_shard.add_to_shard(key, value, false);
//                     }
                    
//                     res.set_content(to_string(value), "text/plain");
                    
//                 } catch (const exception& e) {
//                     res.status = 500;
//                 }
//             });

//             // POST: FAST - Async write-back
//             svr.Post("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
//                 try {
//                     int key = stoi(req.path_params.at("key"));
//                     int value = stoi(req.path_params.at("value"));
//                     int shard_index = get_shard_mapping(key);
//                     auto& current_shard = cache_shards[shard_index];
                    
//                     total_writes++;
                    
//                     // Update cache immediately as DIRTY
//                     {
//                         unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
//                         current_shard.add_to_shard(key, value, true);
//                     }
                    
//                     // Queue for async write-back (non-blocking!)
//                     {
//                         lock_guard<mutex> lk(write_back_lock);
//                         write_back_queue.push(make_pair(key, value));
//                         write_backs_queued++;
//                         cv_write_back.notify_one();
//                     }
                    
//                     res.status = 201;
                    
//                 } catch (...) {
//                     res.status = 500;
//                 }
//             });
            
//             // PUT: FAST - Async write-back
//             svr.Put("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
//                 try {
//                     int key = stoi(req.path_params.at("key"));
//                     int value = stoi(req.path_params.at("value"));
//                     int shard_index = get_shard_mapping(key);
//                     auto& current_shard = cache_shards[shard_index];

//                     total_writes++;
                    
//                     {
//                         unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
//                         current_shard.update_value(key, value, true);
//                     }
                    
//                     {
//                         lock_guard<mutex> lk(write_back_lock);
//                         write_back_queue.push(make_pair(key, value));
//                         write_backs_queued++;
//                         cv_write_back.notify_one();
//                     }
                    
//                     res.status = 200;
                    
//                 } catch (...) {
//                     res.status = 500;
//                 }
//             });
            
//             svr.Delete("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
//                 try {
//                     int key = stoi(req.path_params.at("key"));
//                     int shard_index = get_shard_mapping(key);
//                     auto& current_shard = cache_shards[shard_index];

//                     // Remove from cache
//                     {
//                         unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
//                         current_shard.evict_from_shard(key, true);
//                     }
                    
//                     // Delete from database immediately
//                     Database_Connector* database = db_pool->get_connection(2000);
//                     if (!database) {
//                         res.status = 503;
//                         return;
//                     }
                    
//                     database->run_query(key, -1, request_t::DELETE);
//                     db_pool->return_connection(database);
                    
//                     res.status = 200;
//                 } catch (...) {
//                     res.status = 500;
//                 }
//             });
            
//             cout << "\n=== Cache Server Configuration ===" << endl;
//             cout << "Address: http://" << ip_address << ":" << port_no << endl;
//             cout << "Shards: " << NUM_SHARDS << endl;
//             cout << "Shard Size: " << SHARD_SIZE << " keys" << endl;
//             cout << "Write Policy: ASYNC Write-back (FAST writes)" << endl;
//             cout << "Read Policy: unique_lock (SLOW reads - serialized)" << endl;
//             cout << "======================================\n" << endl;
            
//             svr.listen("0.0.0.0", port_no);
//         }

//     public:
//         Cache() : db_pool(nullptr) {}
        
//         Cache(string ip_address, int port_no, string host, string user, string password, 
//               string database_name, string table_name) {
//             this->host = host;
//             this->user = user;
//             this->password = password;
//             this->database_name = database_name;
//             this->table_name = table_name;

//             int pool_size = 80;
//             db_pool = new ConnectionPool(pool_size, host, user, password, database_name, table_name);

//             for(size_t i = 0; i < NUM_SHARDS; ++i) {
//                 cache_shards[i].head = NULL;
//                 cache_shards[i].tail = NULL;
//                 cache_shards[i].write_back_queue = &write_back_queue;
//                 cache_shards[i].cv_write_back = &cv_write_back;
//                 cache_shards[i].write_back_lock = &write_back_lock;
//             }
            
//             write_back_thread = thread(&Cache::delayed_write_to_database, this);
//             start_server(ip_address, port_no);
//         }

//         ~Cache() {
//             cout << "\n[Cache] Shutting down..." << endl;
            
//             shutdown_flag.store(true);
//             cv_write_back.notify_all();
            
//             if (write_back_thread.joinable()) {
//                 write_back_thread.join();
//             }
            
//             // Flush dirty entries
//             if (!write_back_queue.empty() && db_pool) {
//                 cout << "[Cache] Flushing " << write_back_queue.size() << " dirty writes..." << endl;
//                 Database_Connector* database = db_pool->get_connection(5000);
//                 if (database) {
//                     while (!write_back_queue.empty()) {
//                         auto kv = write_back_queue.front();
//                         write_back_queue.pop();
//                         database->run_query(kv.first, kv.second, request_t::POST);
//                     }
//                     db_pool->return_connection(database);
//                 }
//             }
            
//             if (db_pool) {
//                 delete db_pool;
//             }
            
//             cout << "\n=== Final Statistics ===" << endl;
//             cout << "Total Reads: " << total_reads.load() << endl;
//             cout << "Total Writes: " << total_writes.load() << endl;
//             cout << "Write-backs Queued: " << write_backs_queued.load() << endl;
//             cout << "[Cache] Shutdown complete" << endl;
//         }
// };

// #endif

#ifndef CACHE_H
#define CACHE_H

#include <bits/stdc++.h>
#include <shared_mutex>
#include <atomic>

#include "cpp-httplib/httplib.h"
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>

#include "cache_metadata.h"
#include "request_t.h"
#include "db_connector.h"

using namespace std;

class Cache {
    private:
        static const size_t NUM_SHARDS = 16;
        static const size_t SHARD_SIZE = 100;
        
        struct ConnectionPool {
            vector<Database_Connector*> all_connections;
            queue<Database_Connector*> available_connections;
            mutex pool_mutex;
            condition_variable pool_cv;
            int pool_size;
            atomic<int> active_connections{0};
            atomic<int> total_timeouts{0};
            atomic<int> total_reconnects{0};
            string host, user, password, database_name, table_name;
            
            thread keep_alive_thread;
            atomic<bool> shutdown{false};
            
            ConnectionPool(int size, string h, string u, string p, string db, string tbl) 
                : pool_size(size), host(h), user(u), password(p), 
                  database_name(db), table_name(tbl) {
                
                cout << "[Pool] Creating " << size << " connections..." << endl;
                
                for (int i = 0; i < pool_size; ++i) {
                    try {
                        auto* conn = new Database_Connector(host, user, password, database_name, table_name);
                        if (conn->is_alive()) {
                            all_connections.push_back(conn);
                            available_connections.push(conn);
                            if ((i+1) % 10 == 0 || i == pool_size - 1) {
                                cout << "[Pool] Created " << (i+1) << "/" << size << endl;
                            }
                        } else {
                            delete conn;
                        }
                    } catch (const exception& e) {
                        cerr << "[Pool] Exception " << (i+1) << endl;
                    }
                }
                
                if (all_connections.empty()) {
                    throw runtime_error("Failed to create any database connections!");
                }
                
                cout << "[Pool] Created " << all_connections.size() << " connections" << endl;
                keep_alive_thread = thread(&ConnectionPool::keep_alive_worker, this);
            }
            
            void keep_alive_worker() {
                while (!shutdown.load()) {
                    this_thread::sleep_for(chrono::minutes(5));
                    
                    if (shutdown.load()) break;
                    
                    vector<Database_Connector*> to_ping;
                    {
                        lock_guard<mutex> lock(pool_mutex);
                        queue<Database_Connector*> temp = available_connections;
                        while (!temp.empty()) {
                            to_ping.push_back(temp.front());
                            temp.pop();
                        }
                    }
                    
                    int pinged = 0;
                    for (auto* conn : to_ping) {
                        if (conn->ping()) pinged++;
                    }
                    
                    if (pinged > 0) {
                        cout << "[Pool] Keep-alive: pinged " << pinged << endl;
                    }
                }
            }
            
            Database_Connector* get_connection(int timeout_ms = 3000) {
                auto start = chrono::steady_clock::now();
                unique_lock<mutex> lock(pool_mutex);
                
                auto deadline = start + chrono::milliseconds(timeout_ms);
                
                if (!pool_cv.wait_until(lock, deadline, [this] { return !available_connections.empty(); })) {
                    total_timeouts++;
                    return nullptr;
                }
                
                auto* conn = available_connections.front();
                available_connections.pop();
                active_connections++;
                
                lock.unlock();
                
                if (!conn->is_alive()) {
                    total_reconnects++;
                    conn->ensure_connection();
                }
                
                return conn;
            }
            
            void return_connection(Database_Connector* conn) {
                if (!conn) return;
                
                lock_guard<mutex> lock(pool_mutex);
                available_connections.push(conn);
                active_connections--;
                pool_cv.notify_one();
            }
            
            ~ConnectionPool() {
                shutdown.store(true);
                if (keep_alive_thread.joinable()) {
                    keep_alive_thread.join();
                }
                
                for (auto* conn : all_connections) {
                    delete conn;
                }
            }
        };
        
        ConnectionPool* db_pool;
        
        struct CacheEntry {
            int value;
            bool dirty;
            
            CacheEntry() : value(0), dirty(false) {}
            CacheEntry(int v, bool d) : value(v), dirty(d) {}
        };
        
        struct Shard {
            unordered_map<int, CacheEntry> cache_lines;
            shared_mutex shard_lock;

            // Write-back queue (for dirty writes)
            queue<pair<int, int>> *write_back_queue;
            condition_variable *cv_write_back;
            mutex *write_back_lock;
            
            // NEW: LRU update queue (for async access tracking)
            queue<int> *lru_update_queue;
            condition_variable *cv_lru_update;
            mutex *lru_update_lock;
            
            typedef struct key_node {
                int key;
                struct key_node *previous;
                struct key_node *next;
            } key_node;

            key_node *head, *tail;
            map<int, key_node *> access_list;
            
            ~Shard() {
                while (head != NULL) {
                    key_node* temp = head;
                    head = head->next;
                    delete temp;
                }
                access_list.clear();
            }

            void remove_from_access_list(int key) {
                if(access_list.find(key) == access_list.end()) return;
                
                key_node *current = access_list[key];
                key_node *previous = current->previous;
                key_node* next = current->next;
                
                access_list.erase(key);

                if(previous != NULL) previous->next = next;
                if(next != NULL) next->previous = previous;

                if(tail == current) tail = previous;
                if(head == current) head = next;

                current->previous = NULL;
                current->next = NULL;
                
                delete current;
            }

            void evict_from_shard(int key, bool update_access_list) {
                if(update_access_list) remove_from_access_list(key);
                if(cache_lines.find(key) == cache_lines.end()) return;
                
                // Only write back if dirty
                if (cache_lines[key].dirty) {
                    std::lock_guard<std::mutex> lk(*write_back_lock);
                    (*write_back_queue).push(make_pair(key, cache_lines[key].value));
                    (*cv_write_back).notify_one();
                }
                
                cache_lines.erase(key);
            }

            void lru() {
                if (tail == NULL) return;
                int key = tail->key;
                evict_from_shard(key, true);
            }

            void move_to_top(int key) {
                if(head != NULL && head->key == key) return;
                
                key_node *current = nullptr;
                
                if(access_list.find(key) != access_list.end()) {
                    current = access_list[key];
                    
                    key_node *previous = current->previous;
                    key_node *next = current->next;
                    
                    if(previous == NULL) return;
                    
                    previous->next = next;
                    if(next != NULL) next->previous = previous;
                    if(current == tail) tail = previous;
                    
                    current->next = head;
                    current->previous = NULL;
                    if(head != NULL) head->previous = current;
                    head = current;
                    
                } else {
                    current = new key_node;
                    current->key = key;
                    current->previous = NULL;
                    current->next = NULL;
                    access_list[key] = current;
                    
                    if(head == NULL) {
                        head = current;
                        tail = current;
                    } else {
                        current->next = head;
                        head->previous = current;
                        head = current;
                    }
                }
            }

            void add_to_shard(int key, int value, bool dirty) {
                // If key already exists, just update it
                if(cache_lines.find(key) != cache_lines.end()) {
                    cache_lines[key].value = value;
                    cache_lines[key].dirty = dirty;
                    move_to_top(key);
                    return;
                }
                
                // Evict if full
                if(cache_lines.size() >= SHARD_SIZE) {
                    lru();
                }
                
                cache_lines[key] = CacheEntry(value, dirty);
                move_to_top(key);
            }
            
            // NEW: Add to shard WITHOUT updating LRU (for async LRU updates)
            void add_to_shard_no_lru(int key, int value, bool dirty) {
                // If key already exists, just update it (no LRU update)
                if(cache_lines.find(key) != cache_lines.end()) {
                    cache_lines[key].value = value;
                    cache_lines[key].dirty = dirty;
                    return;
                }
                
                // Evict if full (this still needs LRU)
                if(cache_lines.size() >= SHARD_SIZE) {
                    lru();
                }
                
                cache_lines[key] = CacheEntry(value, dirty);
                // LRU update will happen asynchronously
            }
            
            void update_value(int key, int value, bool dirty) {
                if(cache_lines.find(key) != cache_lines.end()) {
                    cache_lines[key].value = value;
                    cache_lines[key].dirty = dirty;
                    move_to_top(key);
                } else {
                    add_to_shard(key, value, dirty);
                }
            }
        };

        Shard cache_shards[NUM_SHARDS];
        
        // Write-back queue and thread
        queue<pair<int, int>> write_back_queue;
        condition_variable cv_write_back;
        mutex write_back_lock;
        thread write_back_thread;
        
        // NEW: LRU update queue and thread
        queue<int> lru_update_queue;
        condition_variable cv_lru_update;
        mutex lru_update_lock;
        thread lru_update_thread;
        
        atomic<bool> shutdown_flag{false};
        
        string host, user, password, database_name, table_name;
        
        atomic<uint64_t> cache_hits{0};
        atomic<uint64_t> cache_misses{0};
        atomic<uint64_t> total_reads{0};
        atomic<uint64_t> total_writes{0};
        atomic<uint64_t> write_backs_queued{0};
        atomic<uint64_t> lru_updates_queued{0};  // NEW: track LRU updates

        int get_shard_mapping(int key) {
            return (key % NUM_SHARDS);
        }

        void delayed_write_to_database() {
            while(!shutdown_flag.load()) {
                vector<pair<int, int>> batch;
                
                {
                    std::unique_lock<std::mutex> lk(write_back_lock);
                    
                    cv_write_back.wait_for(lk, chrono::milliseconds(100), 
                        [&]{ return !write_back_queue.empty() || shutdown_flag.load(); });
                    
                    if (shutdown_flag.load() && write_back_queue.empty()) {
                        break;
                    }
                    
                    int batch_size = min(100, (int)write_back_queue.size());
                    for (int i = 0; i < batch_size && !write_back_queue.empty(); ++i) {
                        batch.push_back(write_back_queue.front());
                        write_back_queue.pop();
                    }
                }
                
                if (batch.empty()) continue;
                
                Database_Connector* database = nullptr;
                for (int attempt = 0; attempt < 3; ++attempt) {
                    database = db_pool->get_connection(2000);
                    if (database) break;
                    this_thread::sleep_for(chrono::milliseconds(50));
                }
                
                if (!database) {
                    cerr << "[Write-back] Failed to get DB connection, re-queuing batch" << endl;
                    
                    lock_guard<mutex> lk(write_back_lock);
                    for (const auto& kv : batch) {
                        write_back_queue.push(kv);
                    }
                    continue;
                }
                
                for (const auto& kv : batch) {
                    try {
                        database->run_query(kv.first, kv.second, request_t::POST);
                    } catch (const exception& e) {
                        cerr << "[Write-back] Failed key=" << kv.first << endl;
                    }
                }
                
                db_pool->return_connection(database);
            }
        }
        
        // NEW: Async LRU update worker thread
        void async_lru_update_worker() {
            while(!shutdown_flag.load()) {
                vector<int> batch;
                
                {
                    std::unique_lock<std::mutex> lk(lru_update_lock);
                    
                    // Wait for LRU updates or shutdown
                    cv_lru_update.wait_for(lk, chrono::milliseconds(10), 
                        [&]{ return !lru_update_queue.empty() || shutdown_flag.load(); });
                    
                    if (shutdown_flag.load() && lru_update_queue.empty()) {
                        break;
                    }
                    
                    // Process in batches to reduce lock contention
                    int batch_size = min(50, (int)lru_update_queue.size());
                    for (int i = 0; i < batch_size && !lru_update_queue.empty(); ++i) {
                        batch.push_back(lru_update_queue.front());
                        lru_update_queue.pop();
                    }
                }
                
                if (batch.empty()) continue;
                
                // Group keys by shard to minimize lock acquisitions
                unordered_map<int, vector<int>> shard_keys;
                for (int key : batch) {
                    int shard_idx = get_shard_mapping(key);
                    shard_keys[shard_idx].push_back(key);
                }
                
                // Update LRU for each shard
                for (const auto& [shard_idx, keys] : shard_keys) {
                    auto& shard = cache_shards[shard_idx];
                    
                    // Take write lock to update LRU (but briefly!)
                    unique_lock<shared_mutex> write_lock(shard.shard_lock);
                    
                    for (int key : keys) {
                        // Only update if key still exists in cache
                        if (shard.cache_lines.find(key) != shard.cache_lines.end()) {
                            shard.move_to_top(key);
                        }
                    }
                }
            }
        }

        void start_server(string ip_address, int port_no) {
            httplib::Server svr;
            
            // Health check
            svr.Get("/health", [](const httplib::Request&, httplib::Response& res) {
                res.set_content("OK", "text/plain");
            });
            
            // Stats endpoint
            svr.Get("/stats", [&](const httplib::Request&, httplib::Response& res) {
                stringstream ss;
                ss << "Cache Statistics:\n";
                ss << "Total Reads: " << total_reads.load() << "\n";
                ss << "Cache Hits: " << cache_hits.load() << "\n";
                ss << "Cache Misses: " << cache_misses.load() << "\n";
                ss << "Hit Rate: " << (total_reads.load() > 0 ? 
                    100.0 * cache_hits.load() / total_reads.load() : 0.0) << "%\n";
                ss << "Total Writes: " << total_writes.load() << "\n";
                ss << "Write-backs Queued: " << write_backs_queued.load() << "\n";
                ss << "LRU Updates Queued: " << lru_updates_queued.load() << "\n";
                
                if (db_pool) {
                    ss << "\nConnection Pool:\n";
                    ss << "Active Connections: " << db_pool->active_connections.load() << "\n";
                    ss << "Pool Timeouts: " << db_pool->total_timeouts.load() << "\n";
                    ss << "Reconnections: " << db_pool->total_reconnects.load() << "\n";
                }
                
                res.set_content(ss.str(), "text/plain");
            });

            // GET: FAST with async LRU updates
            svr.Get("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
                try {
                    int key = stoi(req.path_params.at("key"));
                    int shard_index = get_shard_mapping(key);
                    auto& current_shard = cache_shards[shard_index];
                    
                    total_reads++;
                    int value;
                    bool cache_hit = false;
                    
                    // Try cache read with SHARED lock (fast!)
                    {
                        shared_lock<shared_mutex> read_lock(current_shard.shard_lock);
                        
                        if (current_shard.cache_lines.find(key) != current_shard.cache_lines.end()) {
                            value = current_shard.cache_lines[key].value;
                            cache_hit = true;
                        }
                    }
                    // SHARED lock released - other readers can proceed immediately!
                    
                    // ASYNC LRU update - queue it, don't wait!
                    if (cache_hit) {
                        {
                            lock_guard<mutex> lk(lru_update_lock);
                            lru_update_queue.push(key);
                            lru_updates_queued++;
                            cv_lru_update.notify_one();
                        }
                        
                        cache_hits++;
                        res.set_content(to_string(value), "text/plain");
                        return;
                    }
                    
                    // Cache miss - fetch from DB
                    cache_misses++;
                    
                    Database_Connector* database = nullptr;
                    for (int attempt = 0; attempt < 3; ++attempt) {
                        database = db_pool->get_connection(1000);
                        if (database) break;
                        this_thread::sleep_for(chrono::milliseconds(10));
                    }
                    
                    if (!database) {
                        res.status = 503;
                        res.set_content("Service unavailable", "text/plain");
                        return;
                    }
                    
                    try {
                        database->run_query(key, -1, request_t::GET);
                        value = database->current_value;
                    } catch (const exception& e) {
                        db_pool->return_connection(database);
                        res.status = 500;
                        return;
                    }
                    
                    db_pool->return_connection(database);
                    
                    // Add to cache as CLEAN (without LRU update)
                    {
                        unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                        current_shard.add_to_shard_no_lru(key, value, false);
                    }
                    
                    // Queue async LRU update for the newly added key
                    {
                        lock_guard<mutex> lk(lru_update_lock);
                        lru_update_queue.push(key);
                        lru_updates_queued++;
                        cv_lru_update.notify_one();
                    }
                    
                    res.set_content(to_string(value), "text/plain");
                    
                } catch (const exception& e) {
                    res.status = 500;
                }
            });

            // POST: FAST - Async write-back
            svr.Post("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
                try {
                    int key = stoi(req.path_params.at("key"));
                    int value = stoi(req.path_params.at("value"));
                    int shard_index = get_shard_mapping(key);
                    auto& current_shard = cache_shards[shard_index];
                    
                    total_writes++;
                    
                    // Update cache immediately as DIRTY
                    {
                        unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                        current_shard.add_to_shard(key, value, true);
                    }
                    
                    // Queue for async write-back (non-blocking!)
                    {
                        lock_guard<mutex> lk(write_back_lock);
                        write_back_queue.push(make_pair(key, value));
                        write_backs_queued++;
                        cv_write_back.notify_one();
                    }
                    
                    res.status = 201;
                    
                } catch (...) {
                    res.status = 500;
                }
            });
            
            // PUT: FAST - Async write-back
            svr.Put("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
                try {
                    int key = stoi(req.path_params.at("key"));
                    int value = stoi(req.path_params.at("value"));
                    int shard_index = get_shard_mapping(key);
                    auto& current_shard = cache_shards[shard_index];

                    total_writes++;
                    
                    {
                        unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                        current_shard.update_value(key, value, true);
                    }
                    
                    {
                        lock_guard<mutex> lk(write_back_lock);
                        write_back_queue.push(make_pair(key, value));
                        write_backs_queued++;
                        cv_write_back.notify_one();
                    }
                    
                    res.status = 200;
                    
                } catch (...) {
                    res.status = 500;
                }
            });
            
            svr.Delete("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
                try {
                    int key = stoi(req.path_params.at("key"));
                    int shard_index = get_shard_mapping(key);
                    auto& current_shard = cache_shards[shard_index];

                    // Remove from cache
                    {
                        unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                        current_shard.evict_from_shard(key, true);
                    }
                    
                    // Delete from database immediately
                    Database_Connector* database = db_pool->get_connection(2000);
                    if (!database) {
                        res.status = 503;
                        return;
                    }
                    
                    database->run_query(key, -1, request_t::DELETE);
                    db_pool->return_connection(database);
                    
                    res.status = 200;
                } catch (...) {
                    res.status = 500;
                }
            });
            
            cout << "\n=== Cache Server Configuration ===" << endl;
            cout << "Address: http://" << ip_address << ":" << port_no << endl;
            cout << "Shards: " << NUM_SHARDS << endl;
            cout << "Shard Size: " << SHARD_SIZE << " keys" << endl;
            cout << "Write Policy: ASYNC Write-back (FAST writes)" << endl;
            cout << "Read Policy: ASYNC LRU updates (FAST reads with shared_lock)" << endl;
            cout << "======================================\n" << endl;
            
            svr.listen("0.0.0.0", port_no);
        }

    public:
        Cache() : db_pool(nullptr) {}
        
        Cache(string ip_address, int port_no, string host, string user, string password, 
              string database_name, string table_name) {
            this->host = host;
            this->user = user;
            this->password = password;
            this->database_name = database_name;
            this->table_name = table_name;

            int pool_size = 80;
            db_pool = new ConnectionPool(pool_size, host, user, password, database_name, table_name);

            // Initialize shards
            for(size_t i = 0; i < NUM_SHARDS; ++i) {
                cache_shards[i].head = NULL;
                cache_shards[i].tail = NULL;
                
                // Write-back queue references
                cache_shards[i].write_back_queue = &write_back_queue;
                cache_shards[i].cv_write_back = &cv_write_back;
                cache_shards[i].write_back_lock = &write_back_lock;
                
                // LRU update queue references
                cache_shards[i].lru_update_queue = &lru_update_queue;
                cache_shards[i].cv_lru_update = &cv_lru_update;
                cache_shards[i].lru_update_lock = &lru_update_lock;
            }
            
            // Start background threads
            write_back_thread = thread(&Cache::delayed_write_to_database, this);
            lru_update_thread = thread(&Cache::async_lru_update_worker, this);
            
            start_server(ip_address, port_no);
        }

        ~Cache() {
            cout << "\n[Cache] Shutting down..." << endl;
            
            shutdown_flag.store(true);
            cv_write_back.notify_all();
            cv_lru_update.notify_all();
            
            // Wait for background threads
            if (write_back_thread.joinable()) {
                write_back_thread.join();
            }
            if (lru_update_thread.joinable()) {
                lru_update_thread.join();
            }
            
            // Flush dirty entries
            if (!write_back_queue.empty() && db_pool) {
                cout << "[Cache] Flushing " << write_back_queue.size() << " dirty writes..." << endl;
                Database_Connector* database = db_pool->get_connection(5000);
                if (database) {
                    while (!write_back_queue.empty()) {
                        auto kv = write_back_queue.front();
                        write_back_queue.pop();
                        database->run_query(kv.first, kv.second, request_t::POST);
                    }
                    db_pool->return_connection(database);
                }
            }
            
            if (db_pool) {
                delete db_pool;
            }
            
            cout << "\n=== Final Statistics ===" << endl;
            cout << "Total Reads: " << total_reads.load() << endl;
            cout << "Total Writes: " << total_writes.load() << endl;
            cout << "Write-backs Queued: " << write_backs_queued.load() << endl;
            cout << "LRU Updates Queued: " << lru_updates_queued.load() << endl;
            cout << "[Cache] Shutdown complete" << endl;
        }
};

#endif