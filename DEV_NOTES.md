# Developer notes

Hey devs, here are some notes that may be of use to you!

Besides these notes, the [contributions guide](https://github.com/keeps/db-preservation-toolkit/blob/master/.github/contributing.md) also has information that might be useful.

[![Build Status](https://travis-ci.org/keeps/db-preservation-toolkit.png?branch=master)](https://travis-ci.org/keeps/db-preservation-toolkit)

## How to build from source

1. Download the [latest release](https://github.com/keeps/db-preservation-toolkit/releases) or clone the repository.
2. Unzip and open the folder on a command-line terminal
3. Build with Maven `mvn -Dmaven.test.skip clean package`

Binaries will be on the `target` folder.

### Configure access to Oracle Maven repository

The Oracle module uses JDBC Drivers from Oracle's Maven repository.
This repository requires an account, which must be created and configured in order to be used by your maven client.

For more info visit the [registration page](http://www.oracle.com/webapps/maven/register/license.html) and [maven configuration page](https://maven.oracle.com/doc.html).

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

This will install the artifacts locally and they will be used in favour of the ones in KEEPS Artifactory.


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

Example release 2.2.0 and prepare for next version 2.3.0.

1. Run `./scripts/release.sh 2.2.0`
2. Wait for [travis tag build](https://travis-ci.org/keeps/roda/) to be finished and successful
3. Local compile to generate dbptk-app.jar artifact `mvn clean package -Dmaven.test.skip`
4. `gren release --draft -t v2.2.0..v2.1.0`
5. Review release and accept release:
	1. Review issues
	2. Add docker run instructions
	3. Upload dbptk-app.jar artifact
	4. Accept release
6. Run `./scripts/update_changelog.sh 2.2.0`
7. Run `./scripts/prepare_next_version.sh 2.3.0`
