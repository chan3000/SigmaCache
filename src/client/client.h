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
            for(int i = 0; i < num_servers; ++i) {
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
                cache_servers = reinterpret_cast<cache_metadata*>(res->body.data());
            }
        }
        
        void send_request(request_t request_type, int key, int value/* For POST and PUT */) {
            int server_index = get_cache_server(key);
            const auto& server = cache_servers[server_index];
            httplib::Client cli(server.ip_address, server.port_no);
            switch(request_type)
            {
                case GET:
                    /* code */
                    break;
                case POST:
                    break;
                case PUT:
                    break;
                case DELETE:
                    break;
                default:
                    cout << "Send a valid request!\n";
                    cout.flush();
                    break;
            }
        }
};
