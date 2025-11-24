#include <bits/stdc++.h>
#include "cpp-httplib/httplib.h"
#include "cache_metadata.h"

using namespace std;

int main() {
	httplib::Server svr;

	/* Hardcoded Cache server details */
	int num_cache_servers = 2;
	cache_metadata* all_cache_servers = (cache_metadata*)malloc(num_cache_servers * sizeof(cache_metadata));
	strcpy(all_cache_servers[0].ip_address, "localhost");
	all_cache_servers[0].port = 5051;
	all_cache_servers[0].start_key = 1;
	all_cache_servers[0].end_key = 1000; 

	strcpy(all_cache_servers[1].ip_address, "localhost");
	all_cache_servers[1].port = 5052;
	all_cache_servers[1].start_key = 1001;
	all_cache_servers[1].end_key = 2000;

	/* Client requests the cache server details at this end-point */
	svr.Get("/initiate", [&](const httplib::Request& req, httplib::Response& res) {
		string binary_data(reinterpret_cast<const char*>(all_cache_servers), num_cache_servers * sizeof(cache_metadata));
		res.set_content(binary_data, "application/octet-stream");
	});

	std::cout << "Server starting on http://localhost:5050" << std::endl;
	svr.listen("0.0.0.0", 5050);
}