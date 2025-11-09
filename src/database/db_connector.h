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