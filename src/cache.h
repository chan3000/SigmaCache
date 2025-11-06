#include <bits/stdc++.h>
#include "cpp-httplib/httplib.h"
#include "cache_metadata.h"
#include "request_t.h"
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>

using namespace std;

#define PLACEHOLDER 0

class Cache {
    private:
        static size_t CACHE_SIZE = 200;
        unordered_map<int, int> cache_lines;
        
        /* Doubly Linked List to implement LRU Cache Replacement policy */
        typedef struct {
            int key;
            struct key_node *previous;
            struct key_node *next;
        } key_node;

        key_node *head, *tail;
        map<int, key_node *> access_list;

        int fetch_from_db(int key) {
            /* Fetch value from database */
            int value = PLACEHOLDER;

            return value;
        }

        void remove_from_access_list(int key) {
            if(access_list.find(key) == access_list.end()) return;
            key_node *current = access_list[key];
            key_node *previous = current->previous;
            key_node* next = current->next;
            access_list.erase(key);

            if(previous != NULL) previous->next = next;
            if(next != NULL) next->previous = previous;

            current->previous = NULL;
            current->next = NULL;
            
            free(current);
            current = NULL;
        }

        void evict_from_cache(int key, bool access_list) {
            if(cache_lines.find(key) == cache_lines.end()) return;
            cache_lines.erase(key);
            if(access_list) remove_from_access_list(key);
        }

        void lru() {
            /* The key at the tail of the access list is the least recently used key */
            int key = tail->key;
            remove_from_access_list(key);
            evict_from_cache(key, 0);
        }

        /* Keep the recently accessed key at the top of the access list */
        void move_to_top(int key) {
            if(access_list.find(key) == access_list.end()) {
                key_node *new_key = (key_node *)malloc(sizeof(key_node));
                new_key->key = key;
                new_key->previous = NULL;
                new_key->next = NULL;
                access_list[key] = new_key;

                /* Add the new node to the tail of the doubly linked list */
                if(tail == NULL) {  /* Indicates no element in the doubly linked list */
                    tail = new_key;
                    head = new_key;
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
            /* Update the current node as head */
            current->next = head;
            current->previous = NULL;
            head->previous = current;
            head = current;
        }

        void add_to_cache(int key, int value) {
            if(cache_lines.size() == CACHE_SIZE) lru();
            
            /* Add to cache */
            cache_lines[key] = value;

            /* Move key to top of access list */
            move_to_top(key);
        }

        void start_server(string ip_address, int port_no) {
            httplib::Server svr;

            svr.Get("/kv_cache/:key", [](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                if(cache_lines.find(key) == cache_lines.end()) {
                    // res.set_content("-1", "text/plain")
                    /* Fetch from database and populate the cache */
                    int value = PLACEHOLDER;

                    add_to_cache(key, value);
                }
                else {
                    int value = cache_lines[key];
                    /* Move current key to top of access list after access */
                    move_to_top(key);
                    res.set_content(to_string(value), "text/plain");
                }
            });

            svr.Post("/kv_cache/:key/:value", [](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int value = stoi(req.path_params.at("value"));

                cache_lines[key] = value;
            });
            
            svr.Put("/kv_cache/:key/:value", [](const httplib::Request& req, httplib::Response& res) {
                int key = stoi(req.path_params.at("key"));
                int value = stoi(req.path_params.at("value"));

                cache_lines[key] = value;
            });
            
            svr.Delete();
        }

        void handle_request() {

        }
    public:
        Cache();
        Cache(string ip_address, int port_no) {
            cache_lines.clear();
            head = NULL;
            tail = NULL;
            start_server(ip_address, port_no);
        }
};