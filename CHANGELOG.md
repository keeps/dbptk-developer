# Changelog

## Version 4.0.0 Release Candidate 1 (09/09/2025)
#### New Features
- Added support for SIARD 2.2 (exporting, importing, and validating)
  - Exporting to SIARD 2 now exports to SIARD 2.2 rather than SIARD 2.1
  - Previously existing options to limit LOB folder size and file count now apply those limits in accordance with SIARD 2.2 specification
----

## Version 3.1.1 (06/08/2025)
#### Bug fixes

- Fix a bug in ExternalLobsConfiguration types
---

## Version 3.1.0 (06/08/2025)
#### New Features
 
- Added option to only import a specific oracle schema #704
- Support for LOBs referenced in S3 providers (MinIO and AWS) #705

#### Enhancements
- Added object verification before unmarshall on virtual table load.

#### Security
- Several dependency upgrades.
---

## Version 3.0.8 (16/05/2025)
#### Bug fixes
 
- The external lobs filter can be applied in the custom view context #685
---

## Version 3.0.7 (13/05/2025)
#### Bug fixes
 
-  Remove null bytes from strings before insertion on postgreql. #660 
-  Fixed crash on null approx. numerics in PostgreSQL #681 
-  Virtual table correctly handling multiple documents #683
-  Updated lob path when siard-dk has lobs in different media #682 
---

## Version 3.0.6 (02/04/2025)
#### Bug fixes

- NPE when logging more information about an exception cause #675

#### Security
- Several major dependency upgrades to fix security vulnerabilities
---

## Version 3.0.5 (26/03/2025)
#### Enhancements

-  Added warn when unicode is wrong #669

---

## Version 3.0.4 (19/02/2025)
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