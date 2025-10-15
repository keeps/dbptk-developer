# Developer notes

Hey devs, here are some notes that may be of use to you!

Besides these notes, the [contributions guide](https://github.com/keeps/db-preservation-toolkit/blob/master/.github/contributing.md) also has information that might be useful.

[![CI](https://github.com/keeps/dbptk-developer/actions/workflows/CI.yml/badge.svg)](https://github.com/keeps/dbptk-developer/actions/workflows/CI.yml) [![Release](https://github.com/keeps/dbptk-developer/actions/workflows/release.yml/badge.svg)](https://github.com/keeps/dbptk-developer/actions/workflows/release.yml) [![CodeQL](https://github.com/keeps/dbptk-developer/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/keeps/dbptk-developer/actions/workflows/codeql-analysis.yml)

## How to build from source

1. Download the [latest release](https://github.com/keeps/db-preservation-toolkit/releases) or clone the repository.
2. Unzip and open the folder on a command-line terminal
3. Build with Maven `mvn -Dmaven.test.skip clean package`

Binaries will be on the `target` folder.

## How to import DBPTK-Developer as a library

DBPTK-Developer migrate its packages to GitHub packages. Please follow the [Documentation](https://docs.github.com/en/enterprise-server@2.22/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-to-github-packages) provided by GitHub on how to install a package.

## Development environment

To develop we recommend the use of Maven and Eclipse (or Intellij with Eclipse Code Formatter plugin).

The following plugins should be installed in Eclipse:

* [ANSI Escape in Console](http://marketplace.eclipse.org/content/ansi-escape-console) to have coloured output in tests

And the following environment variables should be set:

* **DPT_MYSQL_USER** - MySQL user that must be able to create new users and give them permissions (uses 'root' if not defined)
* **DPT_MYSQL_PASS** - MySQL user's password (uses blank password if not defined)
* **DPT_POSTGRESQL_USER** - PostgreSQL user that must be able to create new users and give them permissions (uses 'postgres' if not defined)
* **DPT_POSTGRESQL_PASS** - PostgreSQL user's password (uses blank password if not defined)

To run PostgreSQL tests, a local PostgreSQL database is required and *postgres* user or another user with permission to create new databases and users can be used. This user must be accessible by IP connection on localhost. The access can be tested with ```psql -U username -h 127.0.0.1 -d postgres -W```.

To run MySQL tests, a local MySQL (or MariaDB) database is required and 'root' user or another user with permission to create new databases and users can be used. This user must be accessible by IP connection on localhost. The access can be tested with ```mysql --user="username" -p --database="mysql" --host="127.0.0.1"```.

### Building common parts that may be used by other projects

Use ```mvn clean install -Pcommon``` to locally install the common artifacts so they can be used by other projects.
Note that this is not necessary unless you do not have access to KEEPS Artifactory or you want to make changes to the common artifacts to use in other projects.

### Updating the DBPTK Bindings package

After changing SIARD XML Schema files, maven must be used to compile a new artifact from the XML Schema (using JAXB). To do this, run ```mvn clean install -Pdbptk-bindings``` from project root folder.

When finalize deploy to GitHub artifactory the new version of the bindings. For that follow the steps:

1. Update the version on the `pom.xml` file in the `dbptk-bindings`
2. Run `mvn clean deploy -Pdbptk-bindings`
3. Update the version in the main `pom.xml` file the changes will be inherited by all modules.

## How to prepare and release a new version

This release build/deploy method requires `gren`:

```
curl https://raw.githubusercontent.com/creationix/nvm/v0.33.8/install.sh | bash
source ~/.nvm/nvm.sh
nvm install v8.11.1
npm install github-release-notes -g
```

### Before releasing

1. Make sure the dependencies are installed by running `gren`
2. Security check: `mvn com.redhat.victims.maven:security-versions:check`
3. Update check: `./scripts/check_versions.sh MINOR`

### Releasing a new version

Example release new 2.2.0 version (up from 2.1.0) and prepare for next version 2.3.0

1. Run `./scripts/release.sh 2.2.0`
2. Wait for [GitHub action build](https://github.com/keeps/dbptk-developer/actions/workflows/release.yml) to be finished and successful
3. `gren release --draft -t v2.1.0..v2.2.0`
4. Review release and accept release:
	1. Review issues
	2. Accept release
5. Run `./scripts/update_changelog.sh 2.2.0`
6. Run `./scripts/prepare_next_version.sh 2.3.0`
