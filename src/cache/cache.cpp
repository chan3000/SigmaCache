#include <bits/stdc++.h>
#include "cache.h" 

int main(int argc, char **argv) {
    string ip_address = argv[1];

    int port_no = stoi(argv[2]);
    string host = ip_address, user = "mbchan", password = "123456", 
            database_name = "KVDatabase", table_name = "kv_store";
    Cache cache_server(ip_address, port_no, host, user, password, database_name, table_name);
}