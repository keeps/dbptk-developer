Database Preservation Toolkit
=============================

The Database Preservation Toolkit allows conversion between Database formats, including connection to live systems, for purposes of digitally preserving databases. The toolkit allows conversion of live or backed-up databases into preservation formats such as **DBML** or **SIARD**, XML-based formats created for the purpose of database preservation. The toolkit also allows conversion of the preservation formats back into live systems to allow the full functionality of databases. For example, it supports a specialized export into MySQL, optimized for PhpMyAdmin, so the database can be fully  experimented using a web interface.

This toolkit was part of the [RODA project](http://www.roda-community.org) and now has been released as a project by its own due to the increasing interest on this particular feature. It is now being further developed in the [EARK project](http://www.eark-project.com/) together with a new version of the SIARD preservation format.

The toolkit is created as a platform that uses input and output modules. Each module supports read and/or write to a particular database format or live system. New modules can easily be added by implementation of a new interface and adding of new drivers.

## EARK and SIARD 2.0

A new version of the this tool, together with a [new version of the SIARD preservation format](http://eark-project.github.io/siard-e-format/), is currently being designed and developed on the [EARK project](http://www.eark-project.com/). A draft specification will be available soon, together with a request for comments. In the meanwhile, if you'd like to know more and even send us use cases and requirements, [contact us](http://www.eark-project.com/contacts).

## Download pre-compiled version

Binaries with all dependencies included:
* [db-preservation-toolkit v2.0-rc2](https://github.com/keeps/db-preservation-toolkit/releases/download/2.0-rc2/db-preservation-toolkit-2.0.0-jar-with-dependencies.jar) (release candidate, [see issues](https://github.com/keeps/db-preservation-toolkit/milestones/2.0.0))
* [db-preservation-toolkit v1.0.1](https://github.com/keeps/db-preservation-toolkit/releases/download/v1.0.1/db-preservation-toolkit-1.0.1-jar-with-dependencies.jar)
* [db-preservation-toolkit v1.0.0](http://keeps.github.io/db-preservation-toolkit/db-preservation-toolkit-1.0.0-jar-with-dependencies.jar)

## How to use

To use the program, open a command-line and try out the following command (replace x.y.z accordingly to the version of the binary in use):

```bash
$ java -jar db-preservation-toolkit-x.y.z-jar-with-dependencies.jar
Usage: dbptk <importModule> [import module options] <exportModule> [export module options]

## Plugin:
    -p, --plugin=plugin.jar    (optional) the file containing a plugin module. Several plugins can be specified, separated by a semi-colon (;)

## Available import modules: -i <module>, --import=module

Import module: MySQLJDBC
    -ih, --ihostname=value    (required) the hostname of the MySQL server
    -idb, --idatabase=value    (required) the name of the database to import from
    -iu, --iusername=value    (required) the name of the user to use in connection
    -ip, --ipassword=value    (required) the password of the user to use in connection
    -ipn, --iport-number=value    (optional) the port that the MySQL server is listening

Import module: PostgreSQLJDBC
    -ih, --ihostname=value    (required) the name of the PostgreSQL server host (e.g. localhost)
    -idb, --idatabase=value    (required) the name of the database to connect to
    -iu, --iusername=value    (required) the name of the user to use in connection
    -ip, --ipassword=value    (required) the password of the user to use in connection
    -ide, --idisable-encryption    (optional) use to turn off encryption in the connection
    -ipn, --iport-number=value    (optional) the port of where the PostgreSQL server is listening, default is 5432

Import module: SIARD1
    -if, --ifile=value    (required) Path to SIARD1 archive file

Import module: SIARD2
    -if, --ifile=value    (required) Path to SIARD2 archive file

## Available export modules: -e <module>, --export=module

Export module: MySQLJDBC
    -eh, --ehostname=value    (required) the hostname of the MySQL server
    -edb, --edatabase=value    (required) the name of the database to import from
    -eu, --eusername=value    (required) the name of the user to use in connection
    -ep, --epassword=value    (required) the password of the user to use in connection
    -epn, --eport-number=value    (optional) the port that the MySQL server is listening

Export module: PostgreSQLJDBC
    -eh, --ehostname=value    (required) the name of the PostgreSQL server host (e.g. localhost)
    -edb, --edatabase=value    (required) the name of the database to connect to
    -eu, --eusername=value    (required) the name of the user to use in connection
    -ep, --epassword=value    (required) the password of the user to use in connection
    -ede, --edisable-encryption    (optional) use to turn off encryption in the connection
    -epn, --eport-number=value    (optional) the port of where the PostgreSQL server is listening, default is 5432

Export module: SIARD1
    -ef, --efile=value    (required) Path to SIARD1 archive file
    -ec, --ecompress    (optional) use to compress the SIARD1 archive file with deflate method
    -ep, --epretty-xml    (optional) write human-readable XML

Export module: SIARD2
    -ef, --efile=value    (required) Path to SIARD2 archive file
    -ec, --ecompress    (optional) use to compress the SIARD2 archive file with deflate method
    -ep, --epretty-xml    (optional) write human-readable XML
```

You have to select an input and an output module, providing for each its configuration.


For example, if you want to connect to a live MySQL database and export its content to SIARD 1.0 format, you can use the following command.

```bash
$ java -jar db-preservation-toolkit-x.y.z-jar-with-dependencies.jar \
-i MySQLJDBC -ihostname=localhost -idb example_db -iu username -ip p4ssw0rd \
-o SIARD1 example.siard
```

## How to build from source

1. Download the [latest stable release](https://github.com/keeps/db-preservation-toolkit/releases).
2. Unzip and open the folder on a command-line terminal
3. Build with Maven `mvn clean package`

Binaries will be on the `target` folder

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
$ java -Xmx3g -jar db-preservation-toolkit-x.y.z-jar-with-dependencies.jar ...
```

The toolkit needs enough memory to put the table structure definition in memory (not the data) and to load each data row or row set, which might include having some BLOBs completely in memory, but this depends on the database driver implementation.

**Main hard drive gets full due to temporary files**

Due to the structure of some export modules (e.g. SIARD) and because we only want to pass throught the database once with minimum amount of used memory, all BLOBs and CLOBs of a database table must be kept on temporary files during the export of a table. This can cause your main disk to get full and the execution to fail. To select a diferent folder for the temporary files, e.g. on a bigger hard drive, use the option `-Djava.io.tmpdir=/path/to/tmpdir`. For example, the following command will use the folder `/media/BIGHD/tmp` as the temporary folder:

```bash
$ java -Djava.io.tmpdir=/media/BIGHD/tmp -jar db-preservation-toolkit-x.y.z-jar-with-dependencies.jar ...
```


## Information & Commercial support

For more information or commercial support, contact [KEEP SOLUTIONS](http://www.keep.pt/contactos/?lang=en).

## Development [![Build Status](https://travis-ci.org/keeps/db-preservation-toolkit.png?branch=master)](https://travis-ci.org/keeps/db-preservation-toolkit)

To develop we recommend the use of Maven and Eclipse.

The following plugins should be installed in Eclipse:

* [ANSI Escape in Console](http://marketplace.eclipse.org/content/ansi-escape-console) to have coloured output in tests

And the following environment variables should be set:

* **DPT_MYSQL_USER** - MySQL user that must be able to create new users and give them permissions (uses 'root' if not defined)
* **DPT_MYSQL_PASS** - MySQL user's password (uses blank password if not defined)
* **DPT_POSTGRESQL_USER** - PostgreSQL user that must be able to create new users and give them permissions (uses 'postgres' if not defined)
* **DPT_POSTGRESQL_PASS** - PostgreSQL user's password (uses blank password if not defined)

To run PostgreSQL tests, a local PostgreSQL database is required and *postgres* user or another user with permission to create new databases and users can be used. This user must be accessible by IP connection on localhost. The access can be tested with ```psql -U username -h localhost -d postgres -W```.

To run MySQL tests, a local MySQL (or MariaDB) database is required and 'root' user or another user with permission to create new databases and users can be used. This user must be accessible by IP connection on localhost. The access can be tested with ```mysql --user="username" -p --database="mysql" --host="localhost"```.

### Changing XML Schema files

After changing SIARD XML Schema files, maven must be used to compile a new artifact from the XML Schema (using JAXB). To do this, run ```mvn clean install -Pdbptk-bindings``` from project root folder.

The jars should now be installed in ```<project_dir>/vendor-libs``` (and also in your local maven repository).
