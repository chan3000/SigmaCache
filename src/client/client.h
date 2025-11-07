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
        cache_metadata * cache_servers;

        int get_cache_server(int key) {
            for(size_t i = 0; i < num_servers; ++i) {
                if(key >= cache_servers[i].start_key && key <= cache_servers[i].end_key) return i;
            }
            cout << "Server not found for given key!!\n";
            cout.flush();
            return -1;
        }

    public:
        Client();
        Client(string ip_address, int port_no) {
            this->ip_address = ip_address;
            this->port_no = port_no; 
        }

        void initiate_connection() {
            httplib::Client cli(this->ip_address, this->port_no);
            auto res = cli.Get("/initiate");
            if(res && res->status == 200) {
                num_servers = res->body.size() / sizeof(cache_metadata);
                cache_servers = (cache_metadata *)malloc(num_servers * sizeof(cache_metadata));
                cache_metadata *temp = reinterpret_cast<cache_metadata*>(res->body.data());
                memcpy(cache_servers, temp, num_servers * sizeof(*temp));
            }

            for (size_t i = 0; i < num_servers; ++i) {
                cout << "  - " << cache_servers[i].ip_address << ":" << cache_servers[i].port
                    << " (range: " << cache_servers[i].start_key 
                    << "-" << cache_servers[i].end_key << ")" << endl;
            }
        }
        
        void send_request(request_t::request_t request_type, int key, int value/* For POST and PUT */) {
            int server_index = get_cache_server(key);
            cout << "Connected to cache: " << cache_servers[server_index].ip_address << ", " << cache_servers[server_index].port << endl;
            httplib::Client cli(cache_servers[server_index].ip_address, cache_servers[server_index].port); 
            switch(request_type)
            {
                case request_t::GET: {
                    cout << "[Client] Sending GET Request\n";
                    auto res = cli.Get("/kv_cache/" + to_string(key));
                    cout << res->body << endl;
                    break;
                }
                case request_t::POST:
                    {break;}
                case request_t::PUT:
                    {break;}
                case request_t::DELETE:
                    {break;}
                default:
                    cout << "Send a valid request!\n";
                    cout.flush();
                    break;
            }
        }
};
