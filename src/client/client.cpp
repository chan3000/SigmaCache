#include <iostream>
#include "cache_metadata.h"
#include "client.h"

int main() {
    // std::random_device rd;
    // std::mt19937 gen(rd());

    // std::uniform_int_distribution<> distrib_1(1, 3);
    
    Client client("localhost", 5050);
    client.initiate_connection();

    
    // for(int i = 0; i < 1; ++i) {
    //     client.send_request(request_t::GET, 1, -1);
    //     client.send_request(request_t::GET, 1, -1);
    //     client.send_request(request_t::POST, 1, 1000);
    //     client.send_request(request_t::GET, 10, -1);
    //     client.send_request(request_t::GET, 10, -1);
    //     client.send_request(request_t::POST, 10, 3000);
    //     client.send_request(request_t::GET, 16, -1);
    //     client.send_request(request_t::GET, 1, -1);
    // }
    client.send_request(request_t::PUT, 1, 2);
    client.send_request(request_t::PUT, 10, 3);
    client.send_request(request_t::PUT, 13, 4);
    client.send_request(request_t::PUT, 16, 5);
    client.send_request(request_t::PUT, 40, 6);

    // client.send_request(request_t::DELETE, 15, 6);
    // client.send_request(request_t::DELETE, 13, 6);
    // client.send_request(request_t::DELETE, 10, 6);
    // client.send_request(request_t::DELETE, 1, 6);
    return 0;
}