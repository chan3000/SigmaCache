#ifndef DB_CONNECTOR_H
#define DB_CONNECTOR_H

#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>
#include <thread>
#include <iostream>
#include <chrono>
#include "request_t.h"

using namespace std;

class Database_Connector {
private:
    sql::mysql::MySQL_Driver *driver;
    sql::Connection *con;
    string host, user, password;
    string table_name;
    string database_name;
    chrono::steady_clock::time_point last_used;

public:
    int current_key;
    int current_value;

    Database_Connector() : driver(nullptr), con(nullptr), current_key(-1), current_value(-1) {
        last_used = chrono::steady_clock::now();
    }
    
    Database_Connector(string host, string user, string password, string database_name, string table_name) 
        : host(host), user(user), password(password), database_name(database_name), table_name(table_name),
          current_key(-1), current_value(-1) {
        
        last_used = chrono::steady_clock::now();
        connect();
    }

    ~Database_Connector() {
        try {
            if (con && !con->isClosed()) {
                con->close();
            }
            delete con;
            con = nullptr;
        } catch (...) {}
    }

    // CRITICAL FIX: Add connection establishment
    bool connect() {
        try {
            driver = sql::mysql::get_mysql_driver_instance();
            
            string connection_url;
            if (host == "localhost" || host == "127.0.0.1") {
                connection_url = "tcp://127.0.0.1:3306";
            } else {
                connection_url = "tcp://" + host + ":3306";
            }
            
            con = driver->connect(connection_url, user, password);
            
            if (con && !con->isClosed()) {
                con->setSchema(database_name);
                
                // Set long timeouts
                sql::Statement* stmt = con->createStatement();
                stmt->execute("SET SESSION wait_timeout=28800");
                stmt->execute("SET SESSION interactive_timeout=28800");
                delete stmt;
                
                last_used = chrono::steady_clock::now();
                return true;
            }
            
            con = nullptr;
            return false;
            
        } catch (sql::SQLException &e) {
            cerr << "[DB] Connection error: " << e.what() << endl;
            con = nullptr;
            return false;
        }
    }

    // CRITICAL FIX: Check if connection is alive
    bool is_alive() {
        if (!con) return false;
        
        try {
            if (con->isClosed()) {
                return false;
            }
            
            // Use isValid() or reconnect() API if available
            return con->isValid();
            
        } catch (...) {
            return false;
        }
    }

    // CRITICAL FIX: Reconnect if needed
    bool ensure_connection() {
        if (is_alive()) {
            return true;
        }
        
        cerr << "[DB] Connection dead, reconnecting..." << endl;
        
        // Close old connection
        try {
            if (con) {
                con->close();
                delete con;
                con = nullptr;
            }
        } catch (...) {}
        
        return connect();
    }

    // CRITICAL FIX: Keep-alive ping
    bool ping() {
        try {
            if (!con || con->isClosed()) {
                return false;
            }
            
            sql::Statement* stmt = con->createStatement();
            sql::ResultSet* res = stmt->executeQuery("SELECT 1");
            delete res;
            delete stmt;
            
            last_used = chrono::steady_clock::now();
            return true;
            
        } catch (sql::SQLException &e) {
            cerr << "[DB] Ping failed: " << e.what() << endl;
            return false;
        }
    }

    void run_query(int key, int value, request_t::request_t request_type) {
        // CRITICAL FIX: Ensure connection is valid
        if (!ensure_connection()) {
            cerr << "[DB] Cannot execute query - no connection" << endl;
            return;
        }

        sql::Statement *stmt = nullptr;
        sql::ResultSet *res = nullptr;
        
        try {
            stmt = con->createStatement();
            string query = "";
            
            switch(request_type) {
                case request_t::GET: {
                    query = "SELECT `key`, `value_data` FROM " + table_name + " WHERE `key` = " + to_string(key) + ";";
                    res = stmt->executeQuery(query);
                    
                    if (res && res->next()) {
                        current_key = res->getInt("key");
                        current_value = res->getInt("value_data");
                    } else {
                        current_key = -1;
                        current_value = -1;
                    }
                    
                    if (res) {
                        delete res;
                        res = nullptr;
                    }
                    break;
                }
                case request_t::PUT: {
                    query = "UPDATE " + table_name + " SET `value_data` = " + to_string(value) + " WHERE `key` = " + to_string(key) + ";";
                    stmt->executeUpdate(query);
                    break;
                }
                case request_t::POST: {
                    query = "INSERT INTO " + table_name + " (`key`, `value_data`) VALUES (" + to_string(key) + ", " + to_string(value) + ")"
                            " ON DUPLICATE KEY UPDATE `value_data` = " + to_string(value) + ";";
                    stmt->executeUpdate(query);
                    break;
                }
                case request_t::DELETE: {
                    query = "DELETE FROM " + table_name + " WHERE `key` = " + to_string(key) + ";";
                    stmt->executeUpdate(query);
                    break;
                }
                default:
                    break;
            }
            
            last_used = chrono::steady_clock::now();
            
        } catch (sql::SQLException &e) {
            //cerr << "[DB] Query error: " << e.what() << endl;
            //cerr << "[DB] Query: " << query << endl;
            
            // Mark connection as potentially dead
            try {
                if (con) con->close();
            } catch (...) {}
        }
        
        if (res) delete res;
        if (stmt) delete stmt;
    }
};

#endif