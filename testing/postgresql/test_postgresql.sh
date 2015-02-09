#!/bin/bash

TEST_DB_SOURCE="db-preservation-toolkit"
TEST_DB_TARGET="db-preservation-toolkit_siard"

TEST_DB_USER="db-preservation-toolkit"
TEST_DB_PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-16};echo;)

SIARD_TEMP_DIR=$(mktemp -d)


function psql() {
  sudo -u postgres psql -q --command="$1"
}

function cleanup() {
  psql "DROP DATABASE \"$TEST_DB_SOURCE\";"
  psql "DROP DATABASE \"$TEST_DB_TARGET\";"
}

echo "Creating test database"
# Create test databases
psql "CREATE DATABASE \"$TEST_DB_SOURCE\";"
psql "CREATE DATABASE \"$TEST_DB_TARGET\";"

# Create test user
psql "CREATE USER '$TEST_DB_USER' WITH PASSWORD '$TEST_DB_PASS';"
psql "GRANT ALL PRIVILEGES ON DATABASE '$TEST_DB_SOURCE' to '$TEST_DB_USER';"
psql "GRANT ALL PRIVILEGES ON DATABASE '$TEST_DB_TARGET' to '$TEST_DB_USER';"


# Initialize test database
sudo -u postgres psql -q $TEST_DB_SOURCE < postgresql_test.sql

# Executing
echo "Executing database convert"
java -jar ../../target/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar \
 -i PostgreSQLJDBC localhost db-reservation-toolkit $TEST_DB_USER $TEST_DB_PASS false \
 -o SIARD $SIARD_TEMP_DIR


# Cleanup
cleanup
