#!/bin/bash

#ARGS:
# source database name
# destination database name
# temporary user username
# temporary user password

#ENVIRONMENT VARS:
# DPT_MYSQL_USER - mysql username for a user with permission to create a new user (defaults to "root")
# DPT_MYSQL_PASS - mysql password for the DPT_MYSQL_USER user (defaults to empty)

TEST_DB_SOURCE="$1"
TEST_DB_TARGET="$2"
TEST_DB_USER="$3"
TEST_DB_PASS="$4"

user=${DPT_MYSQL_USER:-root}
pass="$DPT_MYSQL_PASS"

function sql() {
  #mysql -s --user="$user" --password="$password" --database="mysql" --execute="$1"
  mysql -s --user="$user" --password="$pass" --host="127.0.0.1" --execute="$1"
}

# Create test databases
sql "CREATE DATABASE $TEST_DB_TARGET;"
sql "CREATE DATABASE $TEST_DB_SOURCE;"

# Create test user
sql "CREATE USER '$TEST_DB_USER'@'%' IDENTIFIED BY '$TEST_DB_PASS';"
sql "GRANT ALL PRIVILEGES ON $TEST_DB_SOURCE.* TO '$TEST_DB_USER'@'%' IDENTIFIED BY '$TEST_DB_PASS';"
sql "GRANT ALL PRIVILEGES ON $TEST_DB_TARGET.* TO '$TEST_DB_USER'@'%' IDENTIFIED BY '$TEST_DB_PASS';"
sql "GRANT SELECT ON mysql.user TO '$TEST_DB_USER'@'%' IDENTIFIED BY '$TEST_DB_PASS';"
sql "GRANT SELECT ON mysql.tables_priv TO '$TEST_DB_USER'@'%' IDENTIFIED BY '$TEST_DB_PASS';"

sql "FLUSH PRIVILEGES;"
