#include <iostream>
#include "cache_metadata.h"
#include "client.h"

int main() {
    Client client("localhost", 5050);
    client.initiate_connection();
    client.send_request(request_t::GET, 1, -1);
    client.send_request(request_t::GET, 1, -1);
    client.send_request(request_t::POST, 1, 1000);
    client.send_request(request_t::GET, 10, -1);
    client.send_request(request_t::GET, 10, -1);
    client.send_request(request_t::POST, 10, 3000);
    client.send_request(request_t::GET, 16, -1);
    client.send_request(request_t::GET, 1, -1);
    return 0;
}