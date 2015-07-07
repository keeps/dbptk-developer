#!/bin/bash

cd $(dirname $0)

TEST_DB_SOURCE="dpttest"
TEST_DB_TARGET="dpttest_siard"

TEST_DB_USER="dpttest"
TEST_DB_PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-16};echo;)

SIARD_TEMP_FILE=$(mktemp)

function sql() {
  echo "SQL $2> $1"
  psql $2 -q -U postgres --command="$1"
}

echo "Creating test database"
# Create test databases
sql "CREATE DATABASE \"$TEST_DB_SOURCE\";"
sql "CREATE DATABASE \"$TEST_DB_TARGET\";"

echo "Initialize test database"
psql -U postgres -q $TEST_DB_SOURCE < postgresql_test.sql

# Create test user
sql "CREATE USER \"$TEST_DB_USER\" WITH ENCRYPTED PASSWORD '$TEST_DB_PASS';"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";"

# Allow to access source database
sql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_SOURCE\" TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
sql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
sql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"
sql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_SOURCE"

# Allow to write target database
sql "GRANT ALL PRIVILEGES ON DATABASE \"$TEST_DB_TARGET\" TO \"$TEST_DB_USER\";"
sql "GRANT ALL PRIVILEGES ON SCHEMA public to \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
sql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
sql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"
sql "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO \"$TEST_DB_USER\";" "$TEST_DB_TARGET"

# Executing
JAR=`ls ../../target/db-preservation-toolkit-?.?.?-jar-with-dependencies.jar`

echo "Converting DBMS to SIARD at $SIARD_TEMP_FILE"

java -jar $JAR \
 -i PostgreSQLJDBC localhost $TEST_DB_SOURCE $TEST_DB_USER $TEST_DB_PASS false \
 -o SIARD $SIARD_TEMP_FILE store

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
pg_dump -U postgres $TEST_DB_SOURCE $PG_DUMP_OPTIONS > $DUMP_SOURCE

echo "Dumping target DB to $DUMP_TARGET"
pg_dump -U postgres $TEST_DB_TARGET $PG_DUMP_OPTIONS > $DUMP_TARGET

diff -u $DUMP_SOURCE $DUMP_TARGET | wdiff -nd \
-w $'\033[30;41m' -x $'\033[0m' \
-y $'\033[30;42m' -z $'\033[0m'

echo "Cleaning up"
sql "DROP DATABASE \"$TEST_DB_SOURCE\";"
sql "DROP DATABASE \"$TEST_DB_TARGET\";"
sql "REVOKE ALL PRIVILEGES ON SCHEMA public FROM \"$TEST_DB_USER\";"
sql "DROP ROLE \"$TEST_DB_USER\";"

rm -f $SIARD_TEMP_FILE
