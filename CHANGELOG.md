# Changelog

## Version 2.10.4 (26/04/2023)
#### Bug Fixes

Description are being trimmed of to 30 characters in Microsoft SQL Server #553
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
---

## Version 2.6.2 (04/11/2019)
#### Bug fixes

* Fix #338 MSSQL conversion, error on small decimal values, being set to 0
* Fix #406 Validation SIARD error on requirement 5.10
* Fix #407 validation - DBPTK memory issues (heap size java)

#### Improvements

To solve #407 it was used an external library to remove certain objects from the heap. A new property was added to define the path where to storage the off-heap file. More information can be found at our [wiki](https://github.com/keeps/db-preservation-toolkit/wiki/Application-usage#mapdb-options).
---

## Version 2.6.1 (11/10/2019)
#### Bug fixes

* Fix #403 Change T_6.4-5 requirement validation
* Fix #404 Improve SQL role metadata information fetching 
* Fix #405 SIARD Validation fails on additional checks - foreign keys, triggers and check constraints



---

## Version 2.6.0 (02/10/2019)
#### New Features

* SIARD Validator #353 

Validates a SIARD against its specification and also a set of additional checks.
___

#### Bug fixes

* Fix  #392 DBPTK wrongly notices that a schema exists, but it does not
* Fix #395 SIARD can't validate - possibly due to empty table 
* Fix  #397 LOB options: Schema referenced in column list file, was not found in the database
* Fix #400 Malformed entry in table list

#### Enhancements

* #393 Validating error too generic - "validator expects SIARD2.1"

#### Using the new features:

``dbptk validate --import-file <path-to-siard> [-r <path_to_report> -a <path_to_allowed_types>]``

| Option             | Description   |
| :------------- | :------------- |
| --import-file value     | SIARD file to be validated |
| --report  value   | File where the validation report will be saved |
| --allowed value    | File with allowed data types for the categories UDT or distinct, one per line. |

More information can be found at: https://github.com/keeps/db-preservation-toolkit/wiki/Validation

---

## Version 2.6.0-RC (09/09/2019)
#### New Features

* SIARD Validator #353 

Validates a SIARD against its specification and also a set of additional checks.
___

#### Using the new features:

``dbptk validate --import-file <path-to-siard> [-r <path_to_report> -a <path_to_allowed_types>]``

| Option             | Description   |
| :------------- | :------------- |
| --import-file value     | SIARD file to be validated |
| --report  value   | File where the validation report will be saved |
| --allowed value    | File with allowed data types for the categories UDT or distinct, one per line. |

More information can be found at: https://github.com/keeps/db-preservation-toolkit/wiki/Validation

---

## Version 2.5.0 (06/09/2019)
#### New Features

* Support connecting to DBMSs via an SSH tunnel with password authentication #370
___

### Bug Fixes

* Fix SSH connection problems with PostgreSQL #391 
* Create SIARD of postgres fails, schema name error #387
___
### DBVTK Improved Integrations

* Improve database metadata fetching strategy
* Add an option to test if a custom view query is a valid one #390 
___

#### Using the new features:
##### SSH Connection:
To see the help menu use: ``dbptk help migrate``. To check a specific module, ``dbptk help migrate <module>``
* ``--import-ssh`` or ``--export-ssh`` to enable SSH tunnel
* ``--import-ssh-host``, ``--import-ssh-user``, ``--import-ssh-password``, ``--import-ssh-port`` or ``--export-ssh-host``, ``--export-ssh-user``, ``--export-ssh-password``, ``--export-ssh-port`` 

###### Example:
dbptk migrate --import mysql --import-host localhost --import-user root --import-password 123456 --import-database sakila --import-ssh --import-ssh-host IP Address --import-ssh-user USER --import-ssh-password PASSWORD --import-ssh-port <BY_DEFAULT_IS_22> --export siard-2 --export-file /path/to/siard
---

## 2.5.0-RC (05/08/2019)
#### New Features

* Support connecting to DBMSs via an SSH tunnel with password authentication #370
___

#### Using the new features:
##### SSH Connection:
To see the help menu use: ``dbptk help migrate``. To check a specific module, ``dbptk help migrate <module>``
* ``--import-ssh`` or ``--export-ssh`` to enable SSH tunnel
* ``--import-ssh-host``, ``--import-ssh-user``, ``--import-ssh-password``, ``--import-ssh-port`` or ``--export-ssh-host``, ``--export-ssh-user``, ``--export-ssh-password``, ``--export-ssh-port`` 

###### Example:
dbptk migrate --import mysql --import-host localhost --import-user root --import-password 123456 --import-database sakila --import-ssh --import-ssh-host IP Address --import-ssh-user USER --import-ssh-password PASSWORD --import-ssh-port <BY_DEFAULT_IS_22> --export siard-2 --export-file /path/to/siard