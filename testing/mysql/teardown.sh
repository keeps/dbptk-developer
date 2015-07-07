#!/bin/bash

#ARGS:
# source database name
# destination deatabase name
# temporary user username

#ENVIRONMENT VARS:
# DPT_MYSQL_USER - mysql username for a user with permission to create a new user (defaults to "root")
# DPT_MYSQL_PASS - mysql password for the DPT_MYSQL_USER user (defaults to empty)

TEST_DB_SOURCE="$1"
TEST_DB_TARGET="$2"
TEST_DB_USER="$3"

user=${DPT_MYSQL_USER:-root}
pass="$DPT_MYSQL_PASS"

function sql() {
  #mysql -s --user="$user" --password="$password" --database="mysql" --execute="$1"
  mysql -s --user="$user" --password="$pass" --database="mysql" --execute="$1"
}

sql "DROP DATABASE $TEST_DB_SOURCE;"
sql "DROP DATABASE $TEST_DB_TARGET;"
sql "DROP USER '$TEST_DB_USER'@'localhost';"
