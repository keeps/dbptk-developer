#!/bin/bash

cd $(dirname $0)

TEST_DB_SOURCE="dpttest"
TEST_DB_TARGET="dpttest_siard"

TEST_DB_USER="dpttest"
TEST_DB_PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-16};echo;)

SIARD_TEMP_FILE=$(mktemp)

function psql() {
  echo "SQL $2> $1"
  sudo -u postgres psql $2 -q --command="$1"
}

echo "Creating test database"
# Create test databases
psql "CREATE DATABASE \"$TEST_DB_SOURCE\";"
psql "CREATE DATABASE \"$TEST_DB_TARGET\";"

echo "Initialize test database"
sudo -u postgres psql -q $TEST_DB_SOURCE < postgresql_test.sql

# Create test user
psql "CREATE USER \"$TEST_DB_USER\" WITH ENCRYPTED PASSWORD '$TEST_DB_PASS';"
psql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";"

# Allow to access source database
psql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_SOURCE\" TO \"$TEST_DB_USER\";"
psql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
psql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
psql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
psql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"

# Allow to write target database
psql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_TARGET\" TO \"$TEST_DB_TARGET\";"
psql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
psql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
psql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
psql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"

# Executing
JAR=`ls ../../target/db-preservation-toolkit-?.?.?-jar-with-dependencies.jar`

echo "Converting DBMS to SIARD at $SIARD_TEMP_FILE"

java -jar $JAR \
 -i PostgreSQLJDBC localhost $TEST_DB_SOURCE $TEST_DB_USER $TEST_DB_PASS false \
 -o SIARD $SIARD_TEMP_FILE

echo "Converting SIARD back to DBMS"
java -jar $JAR \
 -i SIARD $SIARD_TEMP_FILE \
 -o PostgreSQLJDBC localhost $TEST_DB_TARGET $TEST_DB_USER $TEST_DB_PASS false

echo "Dumping result and comparing"

DUMP_FOLDER=$(mktemp -d)
DUMP_SOURCE="$DUMP_FOLDER/source.sql"
DUMP_TARGET="$DUMP_FOLDER/target.sql"

PG_DUMP_OPTIONS="--format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces"

echo "Dumping original DB to $DUMP_SOURCE"
sudo -u postgres pg_dump $TEST_DB_SOURCE $PG_DUMP_OPTIONS > $DUMP_SOURCE

echo "Dumping target DB to $DUMP_TARGET"
sudo -u postgres pg_dump $TEST_DB_TARGET $PG_DUMP_OPTIONS > $DUMP_TARGET

meld $DUMP_SOURCE $DUMP_TARGET &

echo "Cleaning up"
psql "DROP DATABASE \"$TEST_DB_SOURCE\";"
psql "DROP DATABASE \"$TEST_DB_TARGET\";"
psql "REVOKE ALL PRIVILEGES ON SCHEMA public FROM \"$TEST_DB_USER\";"
psql "DROP ROLE \"$TEST_DB_USER\";"

rm -f $SIARD_TEMP_FILE
