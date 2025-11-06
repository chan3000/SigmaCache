#include "cpp-httplib/httplib.h"
#include <iostream>

int main() {
    httplib::Client cli("localhost", 8080);

    // Simple GET request
    auto res = cli.Get("/");
    if (res) {
        std::cout << "Status: " << res->status << std::endl;
        std::cout << "Body: " << res->body << std::endl;
    } else {
        std::cout << "Error: " << res.error() << std::endl;
    }

    // GET with path parameter
    auto res2 = cli.Get("/hello/");
    if (res2) {
        std::cout << res2->body << std::endl;
    }

    // GET with query parameters
    auto res3 = cli.Get("/search?q=example");
    if (res3) {
        std::cout << res3->body << std::endl;
    }

    return 0;
}