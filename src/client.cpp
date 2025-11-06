#include "cpp-httplib/httplib.h"
#include <iostream>
#include "cache_metadata.h"

cache_metadata* server_metadata;

void initiate_connection() {
	httplib::Client cli("localhost", 5050);
	auto res = cli.Get("/initiate");
    if(res && res->status == 200) {
		size_t num_servers = res->body.size() / sizeof(cache_metadata);
		cache_metadata* server_metadata = reinterpret_cast<cache_metadata*>(res->body.data());
        
        // cout << "Number of cache servers = " << num_servers << endl;
        // for (size_t i = 0; i < num_servers; ++i) {
        //     cout << "  - " << server_metadata[i].ip_address << ":" << server_metadata[i].port
        //          << " (range: " << server_metadata[i].start_key 
        //          << "-" << server_metadata[i].end_key << ")" << endl;
        // }
    }
}

int main() {
    initiate_connection();
    return 0;
}