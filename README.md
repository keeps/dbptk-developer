Database Preservation Toolkit
=============================

The Database Preservation Toolkit allows conversion between Database formats, including connection to live systems, for purposes of digitally preserving databases. The toolkit allows conversion of live or backed-up databases into preservation formats such as DBML, an XML format created for the purpose of database preservation. The toolkit also allows conversion of the preservation formats back into live systems to allow the full functionality of databases. For example, it supports a specialized export into MySQL, optimized for PhpMyAdmin, so the database can be fully  experimented using a web interface.

This toolkit was part of the [RODA project](http://www.roda-community.org) and now has been released as a project by its own due to the increasing interest on this particular feature. 

The toolkit is created as a platform that uses input and output modules. Each module supports read and/or write to a particular database format or live system. New modules can easily be added by implementation of a new interface and adding of new drivers.


## How to build from source

1. Download the [latest stable release](https://github.com/keeps/db-preservation-toolkit/releases).
2. Unzip and open the folder on a command-line terminal
3. Build with Maven

```bash
$ mvn clean package
```

Binaries will be on the `target` folder

## Download pre-compiled version

Binaries with all dependencies included:
* [db-preservation-toolkit-1.0.0-jar-with-dependencies.jar](http://keeps.github.io/db-preservation-toolkit/db-preservation-toolkit-1.0.0-jar-with-dependencies.jar)

## How to use

To use the program, open a command-line and try out the following command:

```bash
$ java -jar db-preservation-toolkit-1.0.0-jar-with-dependencies.jar 
Synopsys: java -jar roda-common-convert-db.jar -i IMPORT_MODULE [options...] -o EXPORT_MODULE [options...]
Available import modules:
	SQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt
	PostgreSQLJDBC hostName [port] database username password encrypt
	MySQLJDBC hostName [port] database username password
	Oracle12cJDBC hostName port database username password
	DB2JDBC hostname port database username password
	SIARD dir
Available export modules:
	SQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt
	PostgreSQLJDBC hostName [port] database username password encrypt
	MySQLJDBC hostName [port] database username password
	DB2JDBC hostname port database username password
	SIARD dir
```

You have to select an input and an output module, providing for each its configuration.


For example, if you want to connect to a live MySQL database and export its content to DBML format, you can use the following command.

```bash
$ java -jar db-preservation-toolkit-1.0.0-jar-with-dependencies.jar \
-i MySQLJDBC localhost example_db username p4ssw0rd \
-o DBML example_db_dbml_export
```

## Related publications & presentations

* Presentation ["Database migration: CLI"](http://hdl.handle.net/1822/17856) by José Ramalho at "A Pratical Approach to Database Archiving", Danish National Archives, Copenhagen, Denmark, 2012-02-07.
* Presentation ["RODA: a service-oriented digital repository: database archiving"](http://hdl.handle.net/1822/17860) by José Ramalho at "A Pratical Approach to Database Archiving", Danish National Archives, Copenhagen, Denmark, 2012-02-07.
* Presentation ["RODA - Repository of Authentic Digital Objects"](http://hdl.handle.net/1822/7405) by Luis Faria at the International Workshop on Database Preservation, Edinburgh, 2007.
* José Carlos Ramalho, [Relational database preservation through XML modelling](http://hdl.handle.net/1822/7120), in proceedings of the International Workshop on Markup of Overlapping Structures (Extreme Markup 2007), Montréal, Canada, 2007.
* Marta Jacinto, [Bidirectional conversion between XML documents and relational data bases](http://hdl.handle.net/1822/601), in proceedings of the International Conference on CSCW in Design, Rio de Janeiro, 2002.
* Ricardo Freitas, [Significant properties in the preservation of relational databases](http://hdl.handle.net/1822/13702), Springer, 2010.


Other related publications:
* Neal Fitzgerald, ["Using data archiving tools to preserve archival records in business systems – a case study"](http://purl.pt/24107/1/iPres2013_PDF/Using%20data%20archiving%20tools%20to%20preserve%20archival%20records%20in%20business%20systems%20%E2%80%93%20a%20case%20study.pdf), in proceedings of iPRES 2013, Lisbon, 2013.

## Troubleshooting

**Getting exception "java.net.ConnectException: Connection refused"**

Most databases are not configured by default to allow TCP/IP connections. Check your database configuration if it accepts TCP/IP connection and if your IP address is allowed to connect. Also, ensure that the user has permissions to access the database from your IP address.

**Problems importing from Microsoft Access**

To import from Microsoft Access you need to be on a Windows machine with Microsoft Access installed. This is because the current Microsoft Access import module is implemented using ODBC connection. Therefore, you need Windows installed to be able to use ODBC. Also, you need Microsoft Access installed so its ODBC driver is installed on your system.

Furthermore, in order to extract DB structures we need to have access to the internal database table `Msysrelationships`. You need to perform some hacking over the DBMS and this is version dependent. Please follow the instructions described on Microsoft's white paper, which explains how to do this for all Microsoft Access versions: ["Preparing a Microsoft Access Database for Migration"](http://rawgithub.com/keeps/db-preservation-toolkit/master/doc/Preparing_MSAccess_for_Migration.pdf).

**Got error "java.lang.OutOfMemoryError: Java heap space"**

The toolkit might need more memory than it is available by default (normally 64MB). To increase the available memory use the `-Xmx` option. For example, the following command will increase the heap size to 3 GB.

```bash
$ java -Xmx3g -jar db-preservation-toolkit-1.0.0-jar-with-dependencies.jar ...
```

The toolkit needs enough memory to put the table structure definition in memory (not the data) and to load each data row or row set, which might include having some BLOBs completely in memory, but this depends on the database driver implementation.


## Information & Commercial support

For more information or commercial support, contact [KEEP SOLUTIONS](http://www.keep.pt/contactos/?lang=en).

[![Build Status](https://travis-ci.org/keeps/db-preservation-toolkit.png?branch=master)](https://travis-ci.org/keeps/db-preservation-toolkit)
