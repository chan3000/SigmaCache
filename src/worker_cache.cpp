#include <bits/stdc++.h>

#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>

using namespace std;

int main() {
	con = driver->connect("tcp://localhost:3306", "mbchan", "123456");
	con->setSchema("kv_db");

}