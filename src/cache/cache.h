#include <bits/stdc++.h>
#include <shared_mutex>

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
        static const size_t CACHE_SIZE = 256;
        static const size_t NUM_SHARDS = 3;
        static const size_t SHARD_SIZE = 2;
        
        struct Shard {
            unordered_map<int, int> cache_lines;
            shared_mutex shard_lock;

            queue<pair<int, int>> *write_back_queue;
            condition_variable *cv_write_back;
            mutex *write_back_lock;
            
            /* Doubly Linked List to implement LRU Cache Replacement policy */
            typedef struct key_node {
                int key;
                struct key_node *previous;
                struct key_node *next;
            } key_node;

            key_node *head, *tail;
            map<int, key_node *> access_list;

            void remove_from_access_list(int key) {
                if(access_list.find(key) == access_list.end()) return;
                key_node *current = access_list[key];
                key_node *previous = current->previous;
                key_node* next = current->next;
                access_list.erase(key);

                if(previous != NULL) previous->next = next;
                if(next != NULL) next->previous = previous;

                if(tail == current) tail = current->previous;
                if(head == current) head = current->next;

                current->previous = NULL;
                current->next = NULL;
                
                delete current;
                current = NULL;
            }

            void evict_from_shard(int key, bool access_list) {
                /* Remove key from access list */
                cout << "Evicting from shard, key = " << key << endl;
                if(access_list) remove_from_access_list(key);
                if(cache_lines.find(key) == cache_lines.end()) return;
                
                /* Update the cache value to database */
                std::lock_guard<std::mutex> lk(*write_back_lock);
                (*write_back_queue).push(make_pair(key, cache_lines[key]));
                (*cv_write_back).notify_one();
                //database->run_query(key, cache_lines[key], request_t::PUT);
                cache_lines.erase(key);
            }

            void lru() {
                /* The key at the tail of the access list is the least recently used key */
                int key = tail->key;
                cout << "Replacing key = " << key << endl;
                cout << "Key Access Sequence = ";
                auto temp = head;
                int count = 0;
                while(temp != NULL) {
                    cout << temp->key << " ";
                    temp = temp->next;
                    ++count;
                    if(count == 10) break;
                }
                cout << endl;
                evict_from_shard(key, 1);
            }

            /* Keep the recently accessed key at the top of the access list */
            void move_to_top(int key) {
                if(access_list.find(key) == access_list.end()) {
                    // key_node *new_key = (key_node *)malloc(sizeof(key_node));
                    key_node *new_key = new key_node;
                    new_key->key = key;
                    new_key->previous = NULL;
                    new_key->next = NULL;
                    access_list[key] = new_key;

                    /* Add the new node to the tail of the doubly linked list */
                    if(tail == NULL) {  /* Indicates no element in the doubly linked list */
                        tail = new_key;
                        head = new_key;
                        cout << "New head key = " << head->key << endl;
                    } else {
                        tail->next = new_key;
                        new_key->previous = tail;
                        tail = new_key;
                    }
                }

                key_node *current = access_list[key];
                key_node *previous = current->previous;
                key_node *next = current->next;

                if(previous == NULL) return; /* Already at the top */

                previous->next = next;
                if(next != NULL) next->previous = previous;
                if(current->next == NULL) tail = previous; 
                
                /* Update the current node as head */
                current->next = head;
                current->previous = NULL;
                head->previous = current;
                head = current;
            }

            void add_to_shard(int key, int value) {
                /* Replace LRU Cache, if cache is full */
                cout << "Current shard size = " << cache_lines.size() << endl;
                cout << "Adding key = " << key << " to shard\n"; 
                if(cache_lines.find(key) == cache_lines.end() && 
                    cache_lines.size() == SHARD_SIZE) {
                    lru();
                }
                
                /* Add to cache */
                cache_lines[key] = value;

                /* Move key to top of access list */
                move_to_top(key);
            }
        };

        Shard cache_shards[NUM_SHARDS];
        queue<pair<int, int>> write_back_queue;
        condition_variable cv_write_back;
        mutex write_back_lock;
        thread write_back_thread;
        // Database_Connector* database;
        string host, user, password, database_name, table_name;

        int get_shard_mapping(int key) {
            return (key%NUM_SHARDS);
        }

        void delayed_write_to_database() {
            while(1) {
                std::unique_lock<std::mutex> lk(write_back_lock);
                cv_write_back.wait(lk, [&]{ return !write_back_queue.empty(); });
                Database_Connector* database = new Database_Connector(host, user, password, database_name, table_name);
                while(!write_back_queue.empty()) {
                    auto kv = write_back_queue.front();
                    write_back_queue.pop();
                    cout << "Writing to database key = " << kv.first << " value = " << kv.second << endl; 
                    database->run_query(kv.first, kv.second, request_t::POST);
                }
                delete database;
            }
        }

        void start_server(string ip_address, int port_no) {
            httplib::Server svr;

            svr.Get("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int shard_index = get_shard_mapping(key);
                auto& current_shard = cache_shards[shard_index];
                auto& cache_lines =  current_shard.cache_lines;

                unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                cout << "Received GET Request for key = " << key << endl;
                
                if(cache_lines.find(key) == cache_lines.end()) {
                    /* Fetch from database and populate the cache */
                    cout << "Querying database for key = " << key << endl;
                    Database_Connector* database = new Database_Connector(host, user, password, database_name, table_name);
                    database->run_query(key, -1, request_t::GET);
                    int value = database->current_value;
                    delete database;
                    cout << "Value retreived from database = " << value << endl;
                    cout.flush();
                    current_shard.add_to_shard(key, value);
                    res.set_content(to_string(value), "text/plain");
                }
                else {
                    int value = cache_lines[key];
                    cout << "Value retreived from cache = " << value << endl;
                    cout.flush();
                    /* Move current key to top of access list after access */
                    current_shard.move_to_top(key);
                    res.set_content(to_string(value), "text/plain");
                }
            });

            svr.Post("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int value = stoi(req.path_params.at("value"));
                int shard_index = get_shard_mapping(key);
                auto& current_shard = cache_shards[shard_index];
                
                unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                cout << "Received POST Request for key = " << key << endl;

                current_shard.add_to_shard(key, value);
            });
            
            svr.Put("/kv_cache/:key/:value", [&](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int value = stoi(req.path_params.at("value"));
                int shard_index = get_shard_mapping(key);
                auto& current_shard = cache_shards[shard_index];

                unique_lock<shared_mutex> write_lock(current_shard.shard_lock);
                cout << "Received PUT Request for key = " << key << endl;

                current_shard.add_to_shard(key, value);
            });
            
            svr.Delete("/kv_cache/:key", [&](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int shard_index = get_shard_mapping(key);
                auto& current_shard = cache_shards[shard_index];

                unique_lock<shared_mutex> write_lock(current_shard.shard_lock);

                /* Remove the key from cache, access list, database */
                current_shard.evict_from_shard(key, 1);
                Database_Connector* database = new Database_Connector(host, user, password, database_name, table_name);
                database->run_query(key, -1, request_t::DELETE);
                delete database;
            });
            std::cout << "Cache starting on http://" + ip_address + "/" + to_string(port_no) << std::endl;
            svr.listen(ip_address, port_no);
        }

        // void connect_to_database(string host, string user, string password, string database_name, string table_name) {
        //     this->database = new Database_Connector(host, user, password, database_name, table_name);
        // };

    public:
        Cache() {};
        Cache(string ip_address, int port_no, string host, string user, string password, string database_name, string table_name) {
            // connect_to_database(host, user, password, database_name, table_name);
            this->host = host;
            this->user = user;
            this->password = password;
            this->database_name = database_name;
            this->table_name = table_name;

            /* Initialize the shards */
            for(size_t i = 0; i < NUM_SHARDS; ++i) {
                cache_shards[i].head = NULL;
                cache_shards[i].tail = NULL;
                cache_shards[i].write_back_queue = &write_back_queue;
                cache_shards[i].cv_write_back = &cv_write_back;
                cache_shards[i].write_back_lock = &write_back_lock;
            }
            write_back_queue = {};
            write_back_thread = thread(&Cache::delayed_write_to_database, this);
            start_server(ip_address, port_no);
        }

        // void connect_to_database(string host, string user, string password, string database_name, string table_name) {
        //     this->database = new Database_Connector(host, user, password, database_name, table_name);
        // };

        // ~Cache() {
        //     while(!write_back_queue.empty()) {
        //         auto kv = write_back_queue.front();
        //         write_back_queue.pop();
        //         database->run_query(kv.first, kv.second, request_t::PUT);
        //     }
        // }
};