# Changelog

## v2.2.0-RC (09/04/2019)

#### New features:

-  Oracle: Convert SDO_geometry column content to GML [#343](https://github.com/keeps/db-preservation-toolkit/issues/343)
-  Skip the import/export of specific columns [#342](https://github.com/keeps/db-preservation-toolkit/issues/342)
-  Support the new SIARD 2.1 [#329](https://github.com/keeps/db-preservation-toolkit/issues/329)
-  Add support for arrays [#129](https://github.com/keeps/db-preservation-toolkit/issues/129)

#### Enhancements:

-  Inversion of control in module loading [#361](https://github.com/keeps/db-preservation-toolkit/issues/361)

#### Bug Fixes:

-  Undefined datatypes error when migrating view from another DBMS [#345](https://github.com/keeps/db-preservation-toolkit/issues/345)
-  Error converting SQL Server timestamp field [#322](https://github.com/keeps/db-preservation-toolkit/issues/322)

-----

#### Using the new features:

##### To extract GML files from `SDO_GEOMETRY` cells in Oracle DBMS

* Use the parameter `-egml <directory>` or `--export-gml-directory=<directory>` to specify the directory in which the GML files should be created;
* The GML directory parameter can be used when exporting to SIARD 2 (version 2.0 and 2.1);
* One GML file is created per table (and only for tables containing SDO_GEOMETRY data).

##### To skip the import/export of specific columns

Documentation about this feature is available at [project-wiki/List-Tables-Module](https://github.com/keeps/db-preservation-toolkit/wiki/List-Tables-Module)

##### To use the new SIARD 2.1

* **In the import module**
    No action needed. DBPTK will auto-detect the SIARD 2 version.
* **In the export module**
    Defaults to version 2.1;
    2.0 can still be used by using the `-v 2.0` or `--siard-version 2.0` parameter.
* **Extra functionality**
    When using any SIARD modules, the versions are logged before the migration starts like:
    ```
    Importing SIARD version 1.0
    Exporting SIARD version 2.1
    ```


---

## v2.1.0 (12/03/2019)

#### New features:

-  Support exporting database, schema, table and column descriptions [#174](https://github.com/keeps/db-preservation-toolkit/issues/174)

#### Enhancements:

-  Oracle schema/tablespace error. [#215](https://github.com/keeps/db-preservation-toolkit/issues/215)

#### Bug Fixes:

-  NPE when migrating schema without tables [#369](https://github.com/keeps/db-preservation-toolkit/issues/369)
-  Possible bug in sql-server module, failing to get SQL for views [#327](https://github.com/keeps/db-preservation-toolkit/issues/327)
-  Using -ede still tries to use encryption in postgresql export module [#286](https://github.com/keeps/db-preservation-toolkit/issues/286)
-  Exporting to mysql error: Packet for query is too large [#251](https://github.com/keeps/db-preservation-toolkit/issues/251)

---

## v2.0.1 (17/01/2019)

#### Enhancements:

-  Core and module improvements [#366](https://github.com/keeps/db-preservation-toolkit/issues/366)
-  Use Oracle maven repositories [#364](https://github.com/keeps/db-preservation-toolkit/issues/364)
-  Configure continuous integration and deployment [#362](https://github.com/keeps/db-preservation-toolkit/issues/362)

#### Bug Fixes:

-  Possible bug in sql-server module, failing to import triggers. [#325](https://github.com/keeps/db-preservation-toolkit/issues/325)
-  ORA-01000: maximum open cursors exceeded [#315](https://github.com/keeps/db-preservation-toolkit/issues/315)

---

## v2.0.0 (22/06/2017)

#### Enhancements:

- Improved reporting

---

## v2.0.0-rc2 (04/05/2017)

#### Enhancements:

- Improvements related to the [Database Visualization Toolkit](https://github.com/keeps/db-visualization-toolkit)

#### Bug Fixes:

- See [commit messages](https://github.com/keeps/db-preservation-toolkit/compare/2.0.0-rc1...2.0.0-rc2)

---

## v2.0.0-rc1 (02/05/2017)
#### New features:

* Stores files in folders inside the dbptk home directory:
  * a folder for reports (`reports`)
  * a folder for logs (`log`)
  * a folder for module data (`modules`) 
    * currently only the solr export module uses this folder, but it may be used in the future to store temporary and other module-related files
* The dbptk home directory is:
  * a folder `dbptk` inside the current directory (by default)
  * the value defined in the `dbptk.home` java system property

#### Enhancements:

- Improvements related to the [Database Visualization Toolkit](https://github.com/keeps/db-visualization-toolkit)

#### Bug Fixes:

- See [commit messages](https://github.com/keeps/db-preservation-toolkit/compare/2.0.0-beta7.2...2.0.0-rc1)

---

## v2.0.0-beta7.2 (24/02/2017)

#### Enhancements:

- Improvements in Solr module
- Improvements in MS Access module.

#### Bug Fixes:

- See [commit messages](https://github.com/keeps/db-preservation-toolkit/compare/2.0.0-beta7.1...2.0.0-beta7.2)

---

## v2.0.0-beta7.1 (26/01/2017)

#### Enhancements:

- Improved Solr module

#### Bug Fixes:

- See [commit messages](https://github.com/keeps/db-preservation-toolkit/compare/2.0.0-beta6...2.0.0-beta7.1)

---

## v2.0.0-beta6 (16/11/2016)

#### Enhancements:

- All modules: Better use of computing resources;
- SIARD modules: Better handling of temporary files;

#### Bug Fixes:

- See [commit messages](https://github.com/keeps/db-preservation-toolkit/compare/2.0.0-beta5...4c14c37e57a8dd4df2b9f455373b97a759d55827)

---

## v2.0.0-beta5 (05/07/2016)

#### New features:

- Solr: includes Solr export module used by Database Visualization Toolkit

#### Enhancements:

- All modules: Better use of computing resources

#### Bug Fixes:

- All modules: Better handling of NULL values
- Multi-schema DBMS: Better handling of multiple schemas
- MS SQL Server: fixes connection issue in export module
- MS SQL Server: attempts to obtain SQL statement used to create views
- Oracle: fixes exporting of tables with primary keys

---

## v2.0.0-beta4.3 (30/05/2016)

#### Enhancements:

- All modules: Better output, log and report
- All modules: Performance improvements when converting lots of big strings
- SIARD modules: Fixed problems, improved stability and improved performance of XML escaping [#196](https://github.com/keeps/db-preservation-toolkit/issues/196)
- JDBC Export modules: better error handling when creating foreign keys

#### Bug Fixes:

- SIARD-DK: Fixes blank characters [#191](https://github.com/keeps/db-preservation-toolkit/issues/191)
- SIARD-DK: Fixes table namespaces [#195](https://github.com/keeps/db-preservation-toolkit/issues/195)
- Microsoft SQL Server: Fixed some `DECIMAL` values;
- Microsoft SQL Server: Fixed `VARBINARY(MAX)` type;
- Microsoft SQL Server: Better support for schemas other than `dbo` [#199](https://github.com/keeps/db-preservation-toolkit/issues/199)
- JDBC Import modules: Increased permissiveness [#182](https://github.com/keeps/db-preservation-toolkit/issues/182)
- JDBC Import modules: allows importing of timestamps as old as '0001-01-01 00:00:00.0';
- PostgreSQL export module: fixes exporting to different schema than the original one.

---

## v2.0.0-beta4.2 (11/05/2016)

#### New features:

- Permissive approach, failing less often and trying to recover from errors, even if that means some data loss;
- Produce a separate report file with conversion warnings (possible data loss, data type changes, etc);
- Added parameters to SIARD 1 and 2 that allow the specification of database descriptive metadata (database description, data owner, and more).

#### Enhancements:

- All modules: Change normal output to be more user-friendly (most warnings go to the report file and debug information goes to the `dbptk-app.log.txt` file);
- All modules: better data type conversion from DBMS datatypes to SQL standard (99 and 2008) ones;
- All modules: remove dependency that had a security vulnerability (CVE-2012-5783);
- SIARD 1 and 2, MySQL, PostgreSQL: Better support for database descriptive metadata (comment, remarks, etc. see [#174](https://github.com/keeps/db-preservation-toolkit/issues/174) for more information);
- MySQL: Uses existing database or creates one if it does not exist yet.

#### Bug Fixes:

- SIARD-DK: Fixed the automated tests;
- SIARD 1 and 2: Fixed BLOBs and CLOBs;
- MS SQL Server: Fix problems related to NULL values.

---

## v2.0.0-beta4.1 (19/04/2016)

#### Bug Fixes:

- JDBC: logs any structure creation errors without exiting (previously they were fatal errors)

---

## v2.0.0-beta4.0 (16/03/2016)

#### New features:

- SIARD 2: import and export module supports external LOBs, with parameters to distribute LOB based on the number of files in a folder and size of a folder
- SIARD DK: added import module for SIARD DK
- List tables: added a new export module that writes a list of database tables to a file
- SIARD 1 and 2: allows using a file (produced by the List tables module) to select the tables that should be exported to SIARD file

#### Bug Fixes:

- All: ensure that UTF-8 is default
- JDBC: fixed a problem related to LOBs
- SIARD DK: fixed Windows problems

---

## v2.0.0-beta3.2.5 (24/02/2016)

#### Enhancements:

- SIARD-2: Exports some basic user defined types that may exist in oracle and postgresql databases;

#### Bug Fixes:

- SIARD 1, 2 and DK: Fixed reported issues and documentation errors;
- Windows: Fixed more problems specific to windows operative system;

---

## v2.0.0-beta3.2.4 (30/11/2015)

#### New features:

- Access: Added experimental support for Microsoft Access;

#### Enhancements:

- CLI: Better error reporting;

#### Bug Fixes:

- All: Fixed some bugs in application core.
- Windows, SIARD: Fixed creation of SIARD archives on Microsoft Windows;

---

## v2.0.0-beta3.2.0 (17/11/2015)

#### New features:

- JDBC: Added generic JDBC import and export modules;
- Oracle: Added support for Oracle (tested on Oracle Database 11g Release 2);

#### Enhancements:

- SIARD-2: Supports storing LOBs inside the SIARD archive;
- SIARD-2: Uses SQL2003 standard instead of SQL99;
- CLI: Better output and error reporting.

---

## v2.0.0-alpha3.1.0 (03/11/2015)

#### New features:

- SIARD-DK: partial export support (missing LOBs and UDTs)

#### Enhancements:

- SIARD2: better import and export support (still missing LOBs and UDTs)

---

## v2.0.0-alpha2.1.1 (29/09/2015)

#### New features:

- Adds partial support for SIARD2 (missing LOBs and UDTs)
- Adds a new command line interface
- Adds a plugin system

#### Enhancements:

- Uses composition model for SIARD

#### Bug Fixes:

- Fixes some problems with MySQL and PostgreSQL

---

## v2.0.0-alpha2 (03/12/2014)

#### Bug Fixes:

- Fixed log4j configuration
- Fixed (remaining) unclosed streams
- Added proper declaration, in table's schema files, for clobs and blobs types

---

## v2.0.0-alpha1 (03/12/2014)

#### New features:

- Added SIARD input and output modules

#### Bug Fixes:

- Fixed null pointer exception when blobs are missing
- Fixed file descriptor leak

---

## v1.0.1 (19/11/2014)

#### Bug Fixes:

- The DBML generated by the db-preservation-toolkit (xml file) is not semantically valid [#2](https://github.com/keeps/db-preservation-toolkit/issues/2)
- java.lang.NullPointerException when exporting database [#4](https://github.com/keeps/db-preservation-toolkit/issues/4)

---

## v1.0.0 (01/11/2013)

Initial version, ported from the [RODA project](https://github.com/keeps/roda).
