Database Preservation Toolkit
=============================

The Database Preservation Toolkit allows conversion between Database formats, including connection to live systems, for purposes of digitally preserve databases. The toolkit allows conversion of live or backed-up databases into preservation formats such as DBML, and XML format created for the purpose of database preservation. The toolkit also allows conversion of the preservation format into live systems to allow the full functionality of databases. For example, it supports a specialized export into MySQL, optimized for PhpMyAdmin, so the database can fully be experimented using a web interface.


## How to build from source

1. Download the [latest stable release](https://github.com/keeps/db-preservation-toolkit/releases).
2. Unzip and open the folder on a command-line terminal
3. Build with Maven

```bash
$ mvn clean package
```
4. Binaries will be on the `target` folder

## Download pre-compiled version

Binaries with all dependencies included:
* [db-preservation-toolkit-1.0.0-jar-with-dependencies.jar](https://keeps.github.io/db-preservation-toolkit/releases/db-preservation-toolkit-1.0.0-jar-with-dependencies.jar)

## How to use

To use the program, open a command-line and try out the following command:

```bash
$ java -jar db-preservation-toolkit-1.0.0-jar-with-dependencies.jar 
Synopsys: java -jar roda-common-convert-db.jar -i IMPORT_MODULE [options...] -o EXPORT_MODULE [options...]
Available import modules:
	SQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt
	PostgreSQLJDBC hostName database [port] username password encrypt
	MySQLJDBC hostName [port] database username password
	Oracle8i hostName port database username password <- untested!
	MSAccess database.mdb
	ODBC source [username password]
	DBML baseDir
Available export modules:
	SQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt
	PostgreSQLJDBC hostName database [port] username password encrypt
	MySQLJDBC hostName [port] database username password
	PhpMyAdmin hostName [port] database username password
	DBML baseDir
	PostgreSQLFile sqlFile <- SQL file optimized for PostgreSQL
	MySQLFile sqlFile <- SQL file optimized for MySQL
	SQLServerFile sqlFile <- SQL file optimized for SQL Server
	GenericSQLFile sqlFile <- generic SQL file
```

You have to select an input and an output module, providing for each its configuration.


For example, if you want to connect to a live MySQL database and export its content to DBML format, you can use the following command.

```
$ java -jar db-preservation-toolkit-1.0.0-jar-with-dependencies.jar \
-i MySQLJDBC localhost example_db username p4ssw0rd \
-o DBML example_db_dbml_export
```
