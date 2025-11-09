#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>
#include <thread>
#include "request_t.h"

using namespace std;

class Database_Connector{
    private:
        sql::mysql::MySQL_Driver *driver;
        sql::Connection *con;
        // thread_local static sql::Connection* thread_connection;
        string host, user, password;
        string table_name;
        string database_name;

        thread periodic_ping;

    //     sql::Connection* get_thread_connection() {
    //     // First time this thread calls this function?
    //     if (thread_connection == nullptr) {
    //         cout << "[Thread " << this_thread::get_id() 
    //              << "] Creating new connection" << endl;
            
    //         thread_connection = driver->connect(host, user, password);
    //         thread_connection->setSchema(database_name);
            
    //         // sql::Statement *stmt = thread_connection->createStatement();
    //         // stmt->execute("SET SESSION wait_timeout = 86400");
    //         // delete stmt;
    //     }
        
    //     // Test if connection is alive
    //     try {
    //         if (thread_connection->isClosed()) {
    //             cout << "[Thread " << this_thread::get_id() 
    //                  << "] Reconnecting..." << endl;
                
    //             delete thread_connection;
    //             thread_connection = driver->connect(host, user, password);
    //             thread_connection->setSchema(database_name);
    //         }
    //     } catch (...) {
    //         cout << "[Thread " << this_thread::get_id() 
    //              << "] Connection lost, reconnecting..." << endl;
            
    //         delete thread_connection;
    //         thread_connection = driver->connect(host, user, password);
    //         thread_connection->setSchema(database_name);
    //     }
        
    //     return thread_connection;
    // }


        void ping_database() {
            // while(true) {
            //     /* Sleep for n milliseconds */
            //     std::this_thread::sleep_for(std::chrono::milliseconds(100));

            //     // cout << "Cool1 \n";
            //     // cout.flush();
            //     sql::Statement *stmt = con->createStatement();
            //     string query = "UPDATE " + table_name + " SET `value_data` = 0 WHERE `key` = 0;";
            //     stmt->executeUpdate(query);
            //     // cout << "Cool2 \n";
            //     // cout.flush();
            //     delete stmt; 
            //     stmt = NULL;
            // }
        }

        // bool is_connected() {
        //     if(!con) return false;
            
        //     try {
        //         /* Check if connection closed */
        //         if (con->isClosed()) return false;
        //         return true;
                
        //     } catch(sql::SQLException &e) {
        //         return false;
        //     } catch(...) {
        //         return false;
        //     }
        // }


        // void establish_connection() {
        //     bool connected = 0;
        //     if(con != NULL && !con->isClosed()) {
        //         connected = con->isValid();
        //     }

        //     if(!connected) {
        //         delete con;
        //         con = NULL;

        //         cout << "Connection Established\n";

        //         driver = sql::mysql::get_mysql_driver_instance();
        //         con = driver->connect(host, user, password);
        //         con->setSchema(database_name);
        //     }
        // }

        // void establish_connection() {
        //     // Check if already connected
        //     if(is_connected()) return;
            
        //     if(con) {
        //         delete con;
        //         con = nullptr;
        //     }
            
        //     try {
        //         cout << "Establishing database connection..." << endl;
        //         con = driver->connect(host, user, password);
        //         con->setSchema(database_name);
        //         cout << "Database connected successfully" << endl;
                
        //     } catch (sql::SQLException &e) {
        //         cerr << "Connection failed: " << e.what() << endl;
        //         con = nullptr;
        //         throw;
        //     }
        // }

    public:
        Database_Connector() {};
        Database_Connector(string host, string user, string password, string database_name, string table_name) {
            driver = sql::mysql::get_mysql_driver_instance();
            con = driver->connect(host, user, password);
            con->setSchema(database_name);

            // sql::ConnectOptionsMap connection_properties;
            // connection_properties["OPT_RECONNECT"] = true;
            // // Set client-side timeouts
            // connection_properties["OPT_CONNECT_TIMEOUT"] = 30;        // 30 seconds
            // connection_properties["OPT_READ_TIMEOUT"] = 60;           // 60 seconds  
            // connection_properties["OPT_WRITE_TIMEOUT"] = 60;          // 60 seconds
        
            this->host = host;
            this->user = user;
            this->password = password;
            this->database_name = database_name;
            this->table_name = table_name;

            //periodic_ping = thread(&Database_Connector::ping_database, this);
        }

        void run_query(int key, int value, request_t::request_t request_type) {
            // establish_connection();
            // sql::Connection *con = get_thread_connection();
            sql::Statement *stmt = con->createStatement();
            string query = "";
            switch(request_type) {
                case request_t::GET: {
                    query = "SELECT `key`, `value_data` FROM " + table_name + " WHERE `key` = " + to_string(key) + ";";
                    sql::ResultSet *res = stmt->executeQuery(query);
                    while(res->next()) {
                        current_key = res->getInt("key");
                        current_value = res->getInt("value_data");
                    }
                    delete res;
                    res = NULL;
                    break;
                }
                case request_t::PUT: {
                    query = "UPDATE " + table_name + " SET `value_data` = " + to_string(value) + " WHERE `key` = " + to_string(key) + ";";
                    stmt->executeUpdate(query);
                    break;
                }
                case request_t::POST: {
                    query = "INSERT INTO " + table_name + " (`key`, `value_data`) VALUES (" + to_string(key) + ", " + to_string(value) + ")"
                    " ON DUPLICATE KEY UPDATE `value_data` = " + to_string(value)+ ";";
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

            delete stmt;
            delete con;
        }

        int current_key;
        int current_value;
};

// thread_local sql::Connection* Database_Connector::thread_connection = nullptr;