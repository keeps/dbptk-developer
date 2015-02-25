#!/bin/bash

cd $(dirname $0)

TEST_DB_SOURCE="dpttest"
TEST_DB_TARGET="dpttest_siard"

TEST_DB_USER="dpttest"
TEST_DB_PASS=$(< /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c${1:-16};echo;)

SIARD_TEMP_FILE=$(mktemp)

echo -n "MySQL user name: "
read user

echo -n "MySQL user password: "
read -s password

echo

function sql() {
  sql_command=$1

  if [ -z $2 ]; then
    database="$2"
  else
    database="mysql"
  fi

  echo "$database> $1"

  mysql -s --user="$user" --password="$password" --database="$database" --execute="$1"
}

echo "Creating test database"
# Create test databases
sql "CREATE DATABASE $TEST_DB_TARGET;"
sql "CREATE DATABASE $TEST_DB_SOURCE;"

echo "Initialize test database"
mysql --user="$user" --password="$password" --database="$TEST_DB_SOURCE" < mysql_test.sql

# Create test user
sql "CREATE USER '$TEST_DB_USER'@'localhost' IDENTIFIED BY PASSWORD '$TEST_DB_PASS';"
sql "GRANT ALL PRIVILEGES ON *.* TO '$TEST_DB_USER'@'localhost';"
sql "GRANT ALL PRIVILEGES ON *.* TO '$TEST_DB_USER'@'%';"
sql "GRANT ALL PRIVILEGES ON information.schema TO '$TEST_DB_USER'@'localhost';"
sql "GRANT ALL PRIVILEGES ON information.schema TO '$TEST_DB_USER'@'%';"
#sql "GRANT ALL ON $TEST_DB_TARGET.* TO '$TEST_DB_USER'@'%';"
sql "FLUSH PRIVILEGES;"

# Executing
echo "Converting DBMS to SIARD at $SIARD_TEMP_FILE"
# TODO use created users instead of root
#java -jar ../../target/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar \
# -i MySQLJDBC localhost $TEST_DB_SOURCE $TEST_DB_USER $TEST_DB_PASS \
# -o SIARD $SIARD_TEMP_FILE
java -jar ../../target/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar \
 -i MySQLJDBC localhost $TEST_DB_SOURCE $user $password \
 -o SIARD $SIARD_TEMP_FILE

echo "Converting SIARD back to DBMS"
# TODO use created users instead of root
#java -jar ../../target/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar \
# -i SIARD $SIARD_TEMP_FILE \
# -o MySQLJDBC localhost $TEST_DB_TARGET $TEST_DB_USER $TEST_DB_PASS
java -jar ../../target/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar \
 -i SIARD $SIARD_TEMP_FILE \
 -o MySQLJDBC localhost $TEST_DB_TARGET $user $password

#echo "TODO: Dumping result and comparing"

#DUMP_FOLDER=$(mktemp -d)
#DUMP_SOURCE="$DUMP_FOLDER/source.sql"
#DUMP_TARGET="$DUMP_FOLDER/target.sql"

#PG_DUMP_OPTIONS="--format plain --no-owner --no-privileges --column-inserts --no-security-labels --no-tablespaces"

#echo "Dumping original DB to $DUMP_SOURCE"
#sudo -u postgres pg_dump $TEST_DB_SOURCE $PG_DUMP_OPTIONS > $DUMP_SOURCE

#echo "Dumping target DB to $DUMP_TARGET"
#sudo -u postgres pg_dump $TEST_DB_TARGET $PG_DUMP_OPTIONS > $DUMP_TARGET

#meld $DUMP_SOURCE $DUMP_TARGET &

echo "Cleaning up"
sql "DROP DATABASE $TEST_DB_SOURCE;"
#sql "DROP DATABASE $TEST_DB_TARGET;"
#psql "REVOKE ALL PRIVILEGES ON SCHEMA public FROM \"$TEST_DB_USER\";"
sql "DROP USER '$TEST_DB_USER'@'localhost';"

#rm -f $SIARD_TEMP_FILE
