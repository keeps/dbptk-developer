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

if [ -z "$DPT_POSTGRESQL_USER" ]
then
  export PGUSER="postgres"
else
  export PGUSER="$DPT_POSTGRESQL_USER"
fi
export PGPASSWORD="$DPT_POSTGRESQL_PASS"

function sql() {
  psql -q -h 127.0.0.1 -d postgres --command="$1"
}

# Create test databases
sql "CREATE DATABASE \"$TEST_DB_SOURCE\";"
sql "CREATE DATABASE \"$TEST_DB_TARGET\";"

# Create test user
sql "CREATE USER \"$TEST_DB_USER\" WITH ENCRYPTED PASSWORD '$TEST_DB_PASS';"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";"

# Allow to access source database
sql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_SOURCE\" TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_TARGET\" TO \"$TEST_DB_USER\";"
sql "REVOKE CONNECT ON DATABASE \"$TEST_DB_SOURCE\" FROM public;"
sql "REVOKE CONNECT ON DATABASE \"$TEST_DB_TARGET\" FROM public;"

PGDATABASE="$TEST_DB_SOURCE"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";"

PGDATABASE="$TEST_DB_TARGET"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";"
