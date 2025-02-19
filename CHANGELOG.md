# Changelog

## Version 3.0.4 (01/01/1970)

#### Security

- Added a check to SIARD-DK lob paths to avoid path traversal #659 
---

## Version 3.0.3 (18/02/2025)
#### Enhancements

- Upgrade MsAccess driver connector
- Upgrade MySQL driver connector
- Removing error on shutdown hook

#### Security

- Fix security vulnerabilities
---

## Version 3.0.2 (13/02/2025)
#### New features

- Added support to SIARD-DK lobs (#625, #628, #629)
- Added support to SIARD-DK version 128 (#625)

#### Bug Fixes:

- Fix Oracle Number export (#496)
- Fix SIARD Validation when two tables in different schemas have the same name (#608)
- Fix SIARD validation XML parser implementation (#631)
- Add SQL query for materialized views (#636)
- Fallback mechanism when building the Lob path (#649)

#### Enhancements
- Separated SIARD-DK versions 128 and 1007 and their respective support

#### Security
- Fix security vulnerabilities
---

## v3.0.1 (02/08/2024)
#### Bug Fixes:

-  Microsoft SQL Server port configuration is being ignored when using via import config module  (#549)

#### Enhancements

- Add log information when an error occurs during the XML creation #615

#### Security
- Fix security vulnerabilities

Special thanks to @daniel-skovenborg for his contributions to this release
---

## Version 3.0.0 (28/06/2024)
### :warning: Breaking Changes
- Java 21 is now required to use the new dbptk-developer. Applications that utilize dbptk-developer as a library must also be migrated to Java 21.

#### Changes
- Upgraded from Java 8 to Java 21

#### Security:

- [CVE-2024-1597](https://www.cve.org/CVERecord?id=CVE-2024-1597): upgraded postgresql dependency to version 42.7.3
- [CVE-2022-24818](https://www.cve.org/CVERecord?id=CVE-2022-24818): upgraded gt-jdbc-oracle dependency to version 28.5
- Several dependency upgrades to fix other security vulnerabilities.
---

## Version 2.11.0 (23/02/2024)
#### New features


- Add flag option not to generate a validation report when performing a validation #546

#### Enhancements

- Improve tests

#### Bug fixes

- Fix PostgreSQL import types
- Fix compatibility with UNIX exit codes
- Fix compatibility with Windows file path separator 
- Fix SIARD path mapping when LobFolder attribute is missing
- Fix compatibility with Java 8

#### Security
- Several dependency major upgrades to fix security vulnerabilities

Special thanks to @daniel-skovenborg for his contributions to this release
---

## Version 2.10.4 (26/04/2023)
#### Bug Fixes

- Description are being trimmed of to 30 characters in Microsoft SQL Server #553
---

## Version 2.10.3 (08/11/2022)
#### Enhancements
* Set dbname as command line argument #541
---

## Version 2.10.2 (27/09/2022)
#### Enhancements
* Add missing data to LOB cells #533 
---

## Version 2.10.1 (28/06/2022)
#### Bug Fixes

* Materialized option doesn't handle the structure as a table #513

#### Enhancements
* Remove percentage indicator from table progress #514
* Add folder property to table structure #515
---

## Version 2.10.0 (28/04/2022)
#### New Features

* Add a threshold for LOBs saved outside the SIARD file (#504) For more information access our [wiki](https://github.com/keeps/dbptk-developer/wiki/Store-lobs-outside-SIARD).

#### Security

* Security fixes (Upgrade dependencies)
---

## Version 2.9.10 (13/10/2021)
#### Bug Fixes

* Merkle Tree filter malfunction with multi-schema databases #494 
---

## Version 2.9.9 (13/07/2021)
#### Bug Fixes

* This parser does not support specification "dbptk-core" version "2.9.8" #486
* ST_GEOMETRY gives Null Pointer Exception error #487

#### Enhancements
* Improved support for Microsoft SQL Server use integrated login on Windows #488
---

## Version 2.9.8 (08/06/2021)
#### Bug Fixes

* creating a siard do not take Postgresql own materialized views #485 
* Problem with overwriting existing folder when exporting to SIARD-DK #480 

#### Dependencies update

* Bump UCanAccess Driver #481
---

## Version 2.9.7 (06/04/2021)
#### Bug Fixes

* Oracle connection fails if password has special characters #471
* SIARD to SIARD migration ignoring some import-config properties #472
* 'numeric' column type without any precision or scale is assumed to be integer #477
---

## Version 2.9.6 (02/10/2020)
#### Bug Fixes
* Missing a dependency when handling Oracle XMLType #465 
* NPE during validation if mandatory fields are not filled #466
* Fallback message when certain queries return unexpected results #467
* Add strategy to ignore and report triggers that not comply with SIARD 2.1 specification #469
---

## Version 2.9.5 (25/09/2020)
#### Bug Fixes
* Add queryOriginal metadata field to PostgreSQL DBMS #451
* PostgreSQL module not appending where and order by clauses #462
* Normalize file path inside the table content when LOBs outside SIARD file #464 

---

## Version 2.9.4 (28/08/2020)
#### Bug Fixes
* Command line output differs from the file of validation reporter #461
---

## Version 2.9.3 (04/08/2020)
#### Bug Fixes
* Validation fails when the validator runs in parallel #458
---

## Version 2.9.2 (04/08/2020)
#### Bug Fixes
* Fix number column in Oracle with no length specified gives error in extraction of table definition (#454)
* Fix columnConfiguration to include NON_DEFAULT values for merkle tree property

---

## Version 2.9.1 (23/06/2020)
#### Bug Fixes
* Fix #455 Added support to choose threshold lob size 
---

## Version 2.9.0 (15/05/2020)
#### New Features

* Inventory filter module (#444) For more information access our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Inventory-Filter-Module).

#### Bug Fixes
* Fix #450 Siard metadata edition
---

## Version 2.8.2 (15/04/2020)
### DBPTK Developer Integrations

* Add new input type parameters
* Add more information to module parameters
---

## Version 2.8.1 (01/04/2020)
### DBPTK Enterprise Integrations

* Improve DBPTK Enterprise integration when dealing with LOB
---

## Version 2.8.0 (25/03/2020)
#### New Features

* Allow set variables in YAML import-config  (#439) For more information access our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Import-Config-Module#variables).
* Database archive content validation using Merkle Tree. (#433) For more information access our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Merkle-Tree-Filter-Module).

#### Enhancements
* Timestamp output normalization (#440)

#### Improvements
* Bump Jackson version from 2.10.2 to 2.10.3
* Oracle maven dependency is now retrieved from the maven central (eliminates the need to have an Oracle account and configure the maven security to download the dependency)
---

## Version 2.7.0 (18/02/2020)
#### New Features

* Added a new module: __import-config__.This module is used to control which schemas, tables and columns are read from the import module to the export module. It also allows to add options for custom views, views materialization, table or view filtering and database related information such as users, roles, privileges, routines or table oriented like triggers and much more. More information can be found at our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Import-Config-Module).
* Added the capability to filter content from a table (#435)
* Added the capability to sort content from a table (#436)
* Added a Java property to change the prefetch size for Oracle LOB (#437)
* Added a strategy to write LOB data in a parallel fashion (#438)

#### Deprecated

* list-tables module has been replace by the import-config module. More information can be found at our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Import-Config-Module).
---

## Version 2.6.5 (18/02/2020)
__Bug fixes:__

* Fix #422 - Error with package from MS Access (timestamp)
* Fix #423 - NPE when validating a SIARD file foreign keys
* Fix #426 - Description shows "null" when table comment is empty
* Fix #427 - SIARD validation - error when starting two validations at same time
* Fix #428 - Missing encoding on metadata.xml

__Improvements:__

* Add default port number to Oracle connections
---

## Version 2.6.5-RC (27/01/2020)
__Bug fixes:__

* Fix #422 - Error with package from MS Access (timestamp)
* Fix #423 - NPE when validating a SIARD file foreign keys
* Fix #426 - Description shows "null" when table comment is empty
* Fix #427 - SIARD validation - error when starting two validations at same time
* Fix #428 - Missing encoding on metadata.xml

__Improvements:__

* Add default port number to Oracle connections
---

## Version 2.6.4 (26/11/2019)
Bug fixes

* Fix #411 log file and -ide (disable encryption) parameter
* Fix #412 Sybase - Fatal error - NPE
* Fix #414 Opt out which views should be materialized
* Fix #415 List tables missing views
* Fix #418 Problem recreating database from SIARD file (MSSQL Server decimal max precision)

Improvements

* Improve validation reporting - related to #416 
---

## Version 2.6.3 (06/11/2019)
#### Bug fixes

* Fix #409 DBPTK migration - microseconds should not be rounded