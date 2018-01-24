# Testing oracle

## Setup

sign up and get oracle database docker images:

  * from (**preferred**) https://container-registry.oracle.com/
  * from https://github.com/oracle/docker-images
  * from https://store.docker.com/search?certification_status=certified&category=database&q=oracle&source=verified&type=image

## Connecting to oracle databases

Oracle Instant Client is a repackaging of Oracle Database libraries, tools and header files usable to create and run applications that connect to a remote (or local) Oracle Database.

```
docker run -ti --rm container-registry.oracle.com/database/instantclient sqlplus user/passwd@hostOrIp/serviceName
```

## IMAGE: container-registry.oracle.com/database/enterprise:12.2.0.1-slim

The slim (12.2.0.1-slim) version of EE does not have support for Analytics, Oracle R, Oracle Label Security, Oracle Text, Oracle Application Express and Oracle DataVault. The commands to run the Oracle Database Server 12.2.0.1 Enterprise Edition Slim variant should use the same commands but specifying the slim docker image (denoted in the tag)

### Run

```
docker run -d -it --name oracle-db-enterprise -p 1521:1521 -p 5500:5500 container-registry.oracle.com/database/enterprise:12.2.0.1-slim
```

The Database Server is up and ready to use when the STATUS field shows (healthy) in the output of docker ps.

#### Persistance

Ùse this volume:

```
-v /data/OracleDB:/ORCL
```

#### Environment options

In my tests I was not able to make any changes to these, ie: even when setting `DB_DOMAIN` it would still have the default value during runtime.

**DB\_SID** This parameter changes the ORACLE_SID of the database. This variable is optional and the default value is set to ORCLCDB.

**DB\_PDB** - This parameter modifies the name of the PDB. This variable is optional and the default value is set to ORCLPDB1.

**DB\_MEMORY** - This parameter sets the memory requirement for the Oracle Server. This value determines the amount of memory to be allocated for SGA and PGA. This variable is optional and the default value is set to 2GB.

**DB\_DOMAIN** - This parameter sets the domain to be used for Database Server. The default value is localdomain.

#### Logs

```
docker logs oracle-db-enterprise
```

### Connecting


The Database server can be connected to by executing sqlplus from within the container as

```
docker exec -it <oracle-db> bash -c "source /home/oracle/.bashrc; sqlplus /nolog"
```

By default the Database server exposes port 1521 for Oracle client connections over Oracle’s SQL\*Net protocol and port 5500 for Oracle XML DB. sqlplus or any JDBC client can be used to connect to the database server from outside the container.

The default password for `sys` is `Oradoc_db1`

To connect from outside the container using sqlplus:

```
docker run -ti --rm container-registry.oracle.com/database/instantclient sqlplus "sys/Oradoc_db1@\"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<host-ip-address>)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=ORCLPDB1.localdomain)))\" AS SYSDBA"
```

**Important: use ORCLPDB1 to connect, and not ORCLCDB**

Check the environment options above for the defaults.

### Add sample data

Create a tablespace for the HR user

```
CREATE TABLESPACE users DATAFILE 'users.dbf' SIZE 100M AUTOEXTEND ON;

SQL> @?/demo/schema/human_resources/hr_main_new.sql
```

And answer the questions:

```
specify password for HR as parameter 1:
Enter value for 1: Oradoc_db1

specify default tablespeace for HR as parameter 2:
Enter value for 2: users

specify temporary tablespace for HR as parameter 3:
Enter value for 3: temp

specify log path as parameter 4:
Enter value for 4: /u01/app/oracle/product/12.2.0/dbhome_1/demo/schema/log/
```















-------------------------------------------------------------------------------------------------------------------------------


# Untested

## IMAGE: container-registry.oracle.com/database/standard:12.1.0.2

### Minimum requirements

* 25GB of disk space
* 4GB of memory (8GB memory or more is recommended)

### Running

```
docker run -d -e DB_SID=OraDoc -e DB_PASSWD=123456 -e DB_DOMAIN=localhost -e DB_BUNDLE=basic -p 1521:1521 -p 5500:5500 -it --name oracle-db-standard --shm-size="4g" container-registry.oracle.com/database/standard
```

Options:

```
1521 is the listener port

5500 is the http service port

--shm-size="2g" is the memory size for the container to run. The minimum requirement is 4GB

DB_SID (name): default=ORCL (cannot be longer than 8 characters)

DB_PASSWD (db passwd): default=Oracle 

DB_DOMAIN (db domain): default=localdomain

DB_BUNDLE (db bundle): default=basic (valid values: basic / high / extreme; high and extreme are only available for enterprise edition)
```

The first database setup takes about 5 to 8 minutes. Logs are kept under `/home/oracle/setup/log`.

To check whether the database setup is successful, check the log file `/home/oracle/setup/log/setupDB.log`. If `Done ! The database is ready for use.` is shown, the database setup was successful.

The restart of container takes less than 1 minute just to start the database and its listener. The startup log is `/home/oracle/setup/log/startupDB.log`


