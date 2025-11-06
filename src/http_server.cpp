#include "cpp-httplib/httplib.h"
#include <iostream>

int main() {
    httplib::Server svr;

    // Simple GET endpoint
    svr.Get("/", [](const httplib::Request& req, httplib::Response& res) {
        res.set_content("Hello World!", "text/plain");
    });

    // GET with path parameter
    svr.Get("/hello/:name", [](const httplib::Request& req, httplib::Response& res) {
        auto name = req.path_params.at("name");
        res.set_content("Hello " + name + "!", "text/plain");
    });

    // GET with query parameters
    svr.Get("/search", [](const httplib::Request& req, httplib::Response& res) {
        if(req.has_param("q")) {
            auto query = req.get_param_value("q");
            res.set_content("Searching for: " + query, "text/plain");
        } else {
            res.status = 400;
            res.set_content("Missing query parameter", "text/plain");
        }
    });

    std::cout << "Server starting on http://localhost:8080" << std::endl;
    svr.listen("0.0.0.0", 8080);

    return 0;
}