#include <bits/stdc++.h>
#include "cache.h" 

int main(int argc, char **argv) {
    string ip_address = argv[1];
    int port_no = stoi(argv[2]);
    string host = ip_address, user = "mbchan", password = "123456", table_name = "kv_db";
    Cache cache_server(ip_address, port_no, host, user, password, table_name);
}