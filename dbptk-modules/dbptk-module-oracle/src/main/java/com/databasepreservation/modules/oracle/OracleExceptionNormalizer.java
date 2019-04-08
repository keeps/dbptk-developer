/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle;

import java.sql.SQLException;

import com.databasepreservation.model.exception.ConnectionException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.PermissionDeniedException;
import com.databasepreservation.model.exception.ServerException;
import com.databasepreservation.model.modules.ExceptionNormalizer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class OracleExceptionNormalizer implements ExceptionNormalizer {
  private static final OracleExceptionNormalizer instance = new OracleExceptionNormalizer();

  /**
   * Although this class is not necessarily a singleton, it can be used like a
   * singleton to avoid creating multiple (similar) instances.
   *
   * @return an ExceptionNormalizer
   */
  public static ExceptionNormalizer getInstance() {
    return instance;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    if (exception instanceof SQLException) {
      SQLException e = (SQLException) exception;

      // codes found at https://docs.oracle.com/en/database/, then "Browse" then find
      // the codes in the "Error Messages" book for each database. Errors for 11.2,
      // 12.1, 12.2, 18 and 19 included

      String oraCode = e.getMessage().substring(0, 9);
      if (oraCode.matches("ORA-\\d{5}")) {
        switch (oraCode) {
          case "ORA-00038": // Cannot create session: server group belongs to another user
          case "ORA-00362": // member is required to form a valid logfile in group string
          case "ORA-00990": // missing or invalid privilege
          case "ORA-01004": // default username feature not supported; logon denied
          case "ORA-01005": // null password given; logon denied
          case "ORA-01015": // logon called recursively
          case "ORA-01017": // invalid username/password; logon denied
          case "ORA-01031": // insufficient privileges
          case "ORA-01035": // ORACLE only available to users with RESTRICTED SESSION privilege
          case "ORA-01039": // insufficient privileges on underlying objects of the view
          case "ORA-01040": // invalid character in password; logon denied
          case "ORA-01045": // user string lacks CREATE SESSION privilege; logon denied
          case "ORA-01074": // cannot shut down ORACLE; inside a login session - log off first
          case "ORA-01075": // you are currently logged on
          case "ORA-01076": // multiple logons per process not yet supported
          case "ORA-01536": // space quota exceeded for tablespace 'string'
          case "ORA-01568": // cannot set space quota on PUBLIC
          case "ORA-01711": // duplicate privilege listed
          case "ORA-01712": // you cannot grant a privilege which you do not have
          case "ORA-01713": // GRANT OPTION does not exist for that privilege
          case "ORA-01720": // grant option does not exist for 'string.string'
          case "ORA-01749": // you may not GRANT/REVOKE privileges to/from yourself
          case "ORA-01777": // WITH GRANT OPTION not allowed in this system
          case "ORA-01924": // role 'string' not granted or does not exist
          case "ORA-01927": // cannot REVOKE privileges you did not grant
          case "ORA-01928": // GRANT option not granted for all privileges
          case "ORA-01929": // no privileges to GRANT
          case "ORA-01931": // cannot grant string to a role
          case "ORA-01932": // ADMIN option not granted for role 'string'
          case "ORA-01933": // cannot create a stored object using privileges from a role
          case "ORA-01934": // circular role grant detected
          case "ORA-01939": // only the ADMIN OPTION can be specified
          case "ORA-01950": // no privileges on tablespace 'string'
          case "ORA-01951": // ROLE 'string' not granted to 'string'
          case "ORA-01952": // system privileges not granted to 'string'
          case "ORA-01954": // DEFAULT ROLE clause not valid for CREATE USER
          case "ORA-01955": // DEFAULT ROLE 'string' not granted to user
          case "ORA-01956": // invalid command when OS_ROLES are being used
          case "ORA-01987": // client os username is too long
          case "ORA-01988": // remote os logon is not allowed
          case "ORA-01989": // role 'string' not authorized by operating system
          case "ORA-01996": // GRANT failed: password file 'string' is full
          case "ORA-01997": // GRANT failed: user 'string' is IDENTIFIED GLOBALLY
          case "ORA-01998": // REVOKE failed: user SYS always has SYSOPER and SYSDBA
          case "ORA-01999": // password file cannot be updated in SHARED mode
          case "ORA-02001": // user SYS is not permitted to create indexes with freelist groups
          case "ORA-02021": // DDL operations are not allowed on a remote database
          case "ORA-02044": // transaction manager login denied: transaction in progress
          case "ORA-02048": // attempt to begin distributed transaction without logging on
          case "ORA-02186": // tablespace resource privilege may not appear with other privileges
          case "ORA-02187": // invalid quota specification
          case "ORA-02200": // WITH GRANT OPTION not allowed for PUBLIC
          case "ORA-02204": // ALTER, INDEX and EXECUTE not allowed for views
          case "ORA-02205": // only SELECT and ALTER privileges are valid for sequences
          case "ORA-02224": // EXECUTE privilege not allowed for tables
          case "ORA-02225": // only EXECUTE and DEBUG privileges are valid for procedures
          case "ORA-02289": // sequence does not exist
          case "ORA-02305": // only EXECUTE, DEBUG, and UNDER privileges are valid for types
          case "ORA-02347": // cannot grant privileges on columns of an object table
          case "ORA-02423": // schema name does not match schema authorization identifier
          case "ORA-02566": // cannot create duplicated table - insufficient privileges
          case "ORA-02570": // cannot alter user password - insufficient privileges
          case "ORA-02617": // Neither a credential nor login information was specified
          case "ORA-02618": // Both a credential and login information was specified
          case "ORA-03746": // The GSMUSER account does not have SYSDG and SYSBACKUP privileges.
          case "ORA-03866": // The GSMROOTUSER account does not have SYSDG and SYSBACKUP privileges.
          case "ORA-04023": // Object stringstringstringstringstring could not be validated or authorized
          case "ORA-04051": // user string cannot use database link string.string
          case "ORA-04060": // insufficient privileges to execute string
          case "ORA-06127": // NETTCP: unable to change username
          case "ORA-06130": // NETTCP: host access denied
          case "ORA-06131": // NETTCP: user access denied
          case "ORA-06132": // NETTCP: access denied, wrong password
          case "ORA-06134": // NETTCP: file access privilege violation
          case "ORA-06140": // NETTCP: no such user
          case "ORA-06141": // NETTCP: no privilege for user
          case "ORA-06142": // NETTCP: error getting user information
          case "ORA-06564": // object string does not exist
          case "ORA-06575": // Package or function string is in an invalid state
          case "ORA-06598": // insufficient INHERIT PRIVILEGES privilege
          case "ORA-06959": // Buffer I/O quota is too small
          case "ORA-06961": // Insufficient privilege for attempted operation
          case "ORA-07274": // spdcr: access error, access to oracle denied.
          case "ORA-07278": // splon: ops$username exceeds buffer length.
          case "ORA-07577": // no such user in authorization file
          case "ORA-07592": // sspgprv: Error obtaining required privileges
          case "ORA-08185": // Flashback not supported for user SYS
          case "ORA-09272": // remote os logon is not allowed
          case "ORA-09317": // szprv: insufficient privileges
          case "ORA-09797": // Failed to get O/S MAC privileges.
          case "ORA-09816": // Unable to set effective privileges
          case "ORA-09824": // Unable to enable allowmacaccess privilege.
          case "ORA-09825": // Unable to disable allowmacaccess privilege.
          case "ORA-09842": // soacon: Archmon unable to create named pipe.
          case "ORA-09918": // Unable to get user privileges from SQL*Net
          case "ORA-09919": // Unable to set label of dedicated server
          case "ORA-09927": // Unable to set label of server
          case "ORA-09949": // Unable to get client operating system privileges
          case "ORA-09950": // Unable to get server operating system privileges
          case "ORA-10063": // disable usage of DBA and OPER privileges in osds
          case "ORA-12317": // logon to database (link name string) denied
          case "ORA-12336": // cannot login to database (link name string)
          case "ORA-12345": // user string lacks CREATE SESSION privilege in database link (linkname string)
          case "ORA-12404": // invalid privilege string: string
          case "ORA-12406": // unauthorized SQL statement for policy string
          case "ORA-12407": // unauthorized operation for policy string
          case "ORA-12425": // cannot apply policies or set authorizations for system schemas
          case "ORA-12440": // insufficient authorization for the SYSDBA package
          case "ORA-12446": // Insufficient authorization for administration of policy string
          case "ORA-12465": // not authorized for read or write on specified groups or compartments
          case "ORA-12466": // default level is greater than the user's maximum
          case "ORA-12470": // NULL or invalid user label: string
          case "ORA-12471": // Specified compartment or group is not authorized for user
          case "ORA-12480": // specified clearance labels not within the effective clearance
          case "ORA-12546": // TNS:permission denied
          case "ORA-12549": // TNS:operating system resource quota exceeded
          case "ORA-12555": // TNS:permission denied
          case "ORA-12574": // TNS:redirection denied
          case "ORA-12631": // Username retrieval failed
          case "ORA-12632": // Role fetch failed
          case "ORA-12633": // No shared authentication services
          case "ORA-12635": // No authentication adapters available
          case "ORA-12638": // Credential retrieval failed
          case "ORA-12639": // Authentication service negotiation failed
          case "ORA-12640": // Authentication adapter initialization failed
          case "ORA-12641": // Authentication service failed to initialize
          case "ORA-12642": // No session key
          case "ORA-12647": // Authentication required
          case "ORA-12653": // Authentication control function failed
          case "ORA-12654": // Authentication conversion failed
          case "ORA-12655": // Password check failed
          case "ORA-12656": // Cryptographic checksum mismatch
          case "ORA-12657": // No algorithms installed
          case "ORA-12662": // proxy ticket retrieval failed
          case "ORA-12666": // Dedicated server: outbound transport protocol different from inbound
          case "ORA-12667": // Shared server: outbound transport protocol different from inbound
          case "ORA-12670": // Incorrect role password
          case "ORA-12671": // Shared server: adapter failed to save context
          case "ORA-12672": // Database logon failure
          case "ORA-12677": // Authentication service not supported by database link
          case "ORA-12681": // Login failed: the SecurID card does not have a pincode yet
          case "ORA-12687": // Credentials expired.
          case "ORA-12688": // Login failed: the SecurID server rejected the new pincode
          case "ORA-12690": // Server Authentication failed, login cancelled
          case "ORA-12696": // Double Encryption Turned On, login disallowed
          case "ORA-13027": // unable to read dimension definition from string
          case "ORA-13181": // unable to determine length of column string_SDOINDEX.SDO_CODE
          case "ORA-13202": // failed to create or insert into the SDO_INDEX_METADATA table
          case "ORA-13204": // failed to create spatial index table
          case "ORA-13210": // error inserting data into the index table
          case "ORA-13230": // failed to create temporary table [string] during R-tree creation
          case "ORA-13231": // failed to create index table [string] during R-tree creation
          case "ORA-13250": // insufficient privileges to modify metadata table entries
          case "ORA-13566": // failure to create a subdirectory under directory [string]
          case "ORA-13570": // unable to read label file (%0!s) in the directory (%1!s)
          case "ORA-13616": // The current user string has not been granted the ADVISOR privilege.
          case "ORA-13628": // Insufficient privileges to access the task belonging to the specified user
          case "ORA-13643": // The task can not be interrupted or cancelled.
          case "ORA-13750": // User "string" has not been granted the "ADMINISTER SQL TUNING SET" privilege.
          case "ORA-13752": // User "string" must be SYS or must have the "ADMINISTER ANY SQL TUNING SET"
                            // privilege.
          case "ORA-13773": // insufficient privileges to select data from the cursor cache
          case "ORA-13774": // insufficient privileges to select data from the workload repository
          case "ORA-13776": // User "string" has not been granted the "SELECT" privilege on the "SQL tuning
                            // set" DBA views.
          case "ORA-13842": // no SELECT privilege on DBA_SQL_PROFILES
          case "ORA-14136": // ALTER TABLE EXCHANGE restricted by fine-grained security
          case "ORA-14700": // Object(s) owned by SYS cannot be locked by non-SYS user
          case "ORA-14910": // Sessions with SYSDBA privilege are not allowed to connect to a service that
                            // has commit outcome enabled.
          case "ORA-15055": // unable to connect to ASM instance
          case "ORA-15260": // permission denied on ASM disk group
          case "ORA-15294": // SYSASM privilege not allowed on database instance
          case "ORA-15373": // SYSDG, SYSKM, SYSRAC, and SYSBACKUP administrative privileges cannot be
                            // granted on an ASM instance.
          case "ORA-15552": // workload replay client cannot login to database server
          case "ORA-15553": // workload replay client cannot execute the DBMS_WORKLOAD_REPLAY package
          case "ORA-15567": // replay user string encountered an error during a sanity check
          case "ORA-15568": // login of user string during workload replay failed with ORA-string
          case "ORA-15746": // Missing EXECUTE privilege on DBMS_AUTO_SQLTUNE.
          case "ORA-16191": // Primary log shipping client not logged on standby
          case "ORA-16276": // specified database link does not correspond to primary database
          case "ORA-16279": // supplied dblink does not have sufficient privileges
          case "ORA-16291": // operation not permitted due to incomplete setup
          case "ORA-16493": // using SYSDBA, SYSOPER, SYSDG, or SYSBKUP user for redo shipping to Recovery
                            // Appliance is not supported
          case "ORA-17640": // SQL statement execution failed
          case "ORA-18005": // CREATE ANY OUTLINE privilege is required for this operation
          case "ORA-18006": // DROP ANY OUTLINE privilege is required for this operation
          case "ORA-18007": // ALTER ANY OUTLINE privilege is required for this operation
          case "ORA-18301": // attribute dimension "string"."string" does not exist
          case "ORA-18302": // hierarchy "string"."string" does not exist
          case "ORA-18307": // analytic view "string"."string" does not exist
          case "ORA-19076": // cannot move the XML token set to tablespace where the table owner does not
                            // have privileges
          case "ORA-19374": // invalid staging table
          case "ORA-19375": // no CREATE TABLE privilege on schema "string"
          case "ORA-19376": // no privileges on tablespace provided or tablespace is offline
          case "ORA-22286": // insufficient privileges on file or directory to perform string operation
          case "ORA-22288": // file or LOB operation string failed string
          case "ORA-22928": // invalid privilege on directories
          case "ORA-23322": // Privilege error accessing pipe
          case "ORA-23406": // insufficient privileges on user "string"
          case "ORA-23436": // missing template authorization for user
          case "ORA-23471": // template not authorized for user
          case "ORA-23609": // unable to find directory object for directory string
          case "ORA-23612": // unable to find tablespace "string"
          case "ORA-24016": // cannot create QUEUE_TABLE, user string does not have execute privileges on
                            // QUEUE_PAYLOAD_TYPE string.string
          case "ORA-24048": // cannot create QUEUE_TABLE, user does not have access to AQ object types
          case "ORA-24093": // AQ agent string not granted privileges of database user string
          case "ORA-24164": // invalid rule engine system privilege: string
          case "ORA-24165": // invalid rule engine object privilege: string
          case "ORA-24234": // unable to get source of string "string"."string", insufficient privileges or
                            // does not exist
          case "ORA-24237": // object id argument passed to DBMS_UTILITY.INVALIDATE is not legal
          case "ORA-24242": // anonymous access through a dynamically authenticated Database Access
                            // Descriptor (DAD) prohibited
          case "ORA-24243": // access control entry (ACE) already exists
          case "ORA-24245": // invalid network privilege
          case "ORA-24247": // network access denied by access control list (ACL)
          case "ORA-24252": // SQL translation profile does not exist
          case "ORA-24265": // Insufficient privileges for SQL profile operation
          case "ORA-24301": // null host specified in thread-safe logon
          case "ORA-24313": // user already authenticated
          case "ORA-24366": // migratable user handle is set in service handle
          case "ORA-24367": // user handle has not been set in service handle
          case "ORA-24428": // Sessions with SYSDBA privileges are not supported in this mode.
          case "ORA-24438": // Invalid Authentication Handle specified.
          case "ORA-24530": // User not authorized to access column value.
          case "ORA-24531": // Column value authorization is unknown.
          case "ORA-24536": // Warning - column authorization unknown.
          case "ORA-24542": // PRELIM mode logon not allowed to a pluggable database
          case "ORA-25433": // User string does not have INHERIT REMOTE PRIVILEGES privilege on connected
                            // user string.
          case "ORA-26018": // Column string in table string does not exist
          case "ORA-26723": // user "string" requires the role "string"
          case "ORA-26732": // invalid file group string privilege
          case "ORA-26827": // Insufficient privileges to attach to XStream outbound server "string".
          case "ORA-26854": // string string has no enqueue or dequeue privileges to the queue.
          case "ORA-26855": // STREAMS string has insufficient database privilege to access the queue.
          case "ORA-26856": // STREAMS string has insufficient database privilege to access the queue.
          case "ORA-26887": // Insufficient privileges to attach to XStream inbound server "string".
          case "ORA-26907": // Insufficient privileges to set converge tag
          case "ORA-26922": // user string does not have SELECT ANY TRANSACTION privilege
          case "ORA-26930": // User "string" does not have privileges to perform this operation
          case "ORA-27365": // job has been notified to stop, but failed to do so immediately
          case "ORA-27374": // insufficient privileges on event source queue
          case "ORA-27399": // job type EXECUTABLE requires the CREATE EXTERNAL JOB privilege
          case "ORA-27475": // unknown string "string"."string"
          case "ORA-27486": // insufficient privileges
          case "ORA-27487": // invalid object privilege for a string
          case "ORA-28000": // The account is locked.
          case "ORA-28002": // the password will expire within string days
          case "ORA-28005": // invalid logon flags
          case "ORA-28008": // invalid old password
          case "ORA-28011": // the password has expired; change your password now
          case "ORA-28016": // Privilege string cannot be granted to SYS.
          case "ORA-28019": // audit cannot be configured on administrative privileges
          case "ORA-28021": // cannot grant global roles
          case "ORA-28022": // cannot grant external roles to global user or role
          case "ORA-28023": // must revoke grants of this role to other user(s) first
          case "ORA-28024": // must revoke grants of external roles to this role/user
          case "ORA-28027": // privileged database links may be used by global users
          case "ORA-28029": // could not authorize remote server for user string
          case "ORA-28032": // Your password has expired and the database is set to read-only
          case "ORA-28034": // cannot grant string to an Oracle supplied user
          case "ORA-28035": // Cannot Get Session Key for Authentication
          case "ORA-28037": // Cannot Get Session Key for RACF Authentication
          case "ORA-28038": // disallow O2LOGON
          case "ORA-28040": // No matching authentication protocol
          case "ORA-28046": // Password change for SYS disallowed
          case "ORA-28047": // database is not a member of any enterprise domain in OID
          case "ORA-28048": // database is a member of multiple enterprise domains in OID
          case "ORA-28054": // the password has expired. string Grace logins are left
          case "ORA-28058": // login is allowed only through a proxy
          case "ORA-28081": // Insufficient privileges - the command references a redacted object.
          case "ORA-28111": // insufficient privilege to evaluate policy predicate
          case "ORA-28116": // insufficient privileges to do direct path access
          case "ORA-28132": // The MERGE INTO syntax does not support the security policy.
          case "ORA-28150": // proxy not authorized to connect as client
          case "ORA-28152": // proxy user 'string' may not specify initial role 'string' on behalf of client
                            // 'string'
          case "ORA-28154": // Proxy user may not act as client 'string'
          case "ORA-28156": // Proxy user 'string' not authorized to set role 'string' for client 'string'
          case "ORA-28157": // Proxy user 'string' forbidden to set role 'string' for client 'string'
          case "ORA-28165": // proxy 'string' may not specify password-protected role 'string' for client
                            // 'string'
          case "ORA-28168": // attempted to grant password-protected role
          case "ORA-28178": // password not provided by proxy
          case "ORA-28179": // client user name not provided by proxy
          case "ORA-28180": // multiple authentication methods provided by proxy
          case "ORA-28181": // proxy 'string' failed to enable one or more of the specified initial roles
                            // for client 'string'
          case "ORA-28182": // cannot acquire Kerberos service ticket for client
          case "ORA-28183": // proper authentication not provided by proxy
          case "ORA-28184": // global user cannot have proxy permissions managed in the directory
          case "ORA-28185": // cannot alter user with administrative privilege to proxy-only connect user
          case "ORA-28186": // cannot grant string to a proxy-only connect user
          case "ORA-28190": // SYSRAC administrative privilege cannot be granted to other users
          case "ORA-28191": // cannot grant administrative privileges to a user with non-ASCII or non-EBCDIC
                            // characters in the user name
          case "ORA-28221": // REPLACE not specified
          case "ORA-28222": // may not modify reserved user
          case "ORA-28223": // first login requires password change
          case "ORA-28270": // Malformed user nickname for password authenticated global user.
          case "ORA-28272": // Domain policy restricts password based GLOBAL user authentication.
          case "ORA-28277": // LDAP search, while authenticating global user with passwords, failed.
          case "ORA-28278": // No domain policy registered for password based GLOBAL users.
          case "ORA-28292": // No Domain Policy registered for Kerberos based authentication
          case "ORA-28301": // Domain Policy hasn't been registered for SSL authentication.
          case "ORA-28302": // User does not exist in the LDAP directory service.
          case "ORA-28354": // Encryption wallet, auto login wallet, or HSM is already open
          case "ORA-28390": // auto login wallet not open but encryption wallet may be open
          case "ORA-28395": // could not write the new master key to the wallet
          case "ORA-28401": // Event to disable delay after three failed login attempts
          case "ORA-28404": // role cannot be altered
          case "ORA-28405": // cannot grant secure role to a role
          case "ORA-28417": // password-based keystore is not open
          case "ORA-28418": // password-based HSM is not open
          case "ORA-28441": // RMAN clone instance cannot open wallet
          case "ORA-28447": // insufficient privilege to execute ALTER DATABASE DICTIONARY statement
          case "ORA-28556": // authorization insufficient to access table
          case "ORA-28674": // cannot reference transient index-organized table
          case "ORA-28700": // Only roles can be attached to or detached from program units.
          case "ORA-28702": // Program unit string is not owned by the grantor.
          case "ORA-28704": // Role string is not directly granted to the owner of the program units.
          case "ORA-28705": // The grantor does not have privileges to grant the role string to the program
                            // units.
          case "ORA-28707": // Reserved word ALL was used with roles in the REVOKE command to detach roles
                            // from program units.
          case "ORA-28709": // Roles with DELEGATE option can only be granted to users.
          case "ORA-29283": // invalid file operationstring
          case "ORA-29289": // directory access denied
          case "ORA-29303": // user does not login as SYS
          case "ORA-29397": // cannot grant/revoke switch privilege for string
          case "ORA-29399": // user string does not have privilege to switch to consumer group string
          case "ORA-29471": // DBMS_SQL access denied
          case "ORA-29473": // privilege checking level specified for 'string' must be between 0 and 2
          case "ORA-29475": // useLogonRoles=>TRUE not permitted with userid=>NULL
          case "ORA-29484": // Cannot specify both xs_sessionid and useLogonRoles=>TRUE
          case "ORA-29490": // insufficient privilege
          case "ORA-29520": // name string resolved to a class in schema string that could not be accessed
          case "ORA-29522": // authorization error for referenced name string.string
          case "ORA-29523": // authorization error for unknown referenced name
          case "ORA-29810": // inadequate operator privileges
          case "ORA-29829": // implementation type does not exist
          case "ORA-29830": // operator does not exist
          case "ORA-29837": // insufficient privileges to execute implementation type
          case "ORA-29838": // insufficient privileges to execute the operator(s)
          case "ORA-29882": // insufficient privileges to execute indextype
          case "ORA-29894": // base or varray datatype does not exist
          case "ORA-29972": // user does not have privilege to change/ create registration
          case "ORA-30187": // Permission denied for 'string' to access procedure GET_KEY.
          case "ORA-30446": // valid workload queries not found
          case "ORA-30456": // 'string.string' cannot be refreshed because of insufficient privilege
          case "ORA-30740": // cannot grant UNDER privilege on this object
          case "ORA-30741": // WITH HIERARCHY OPTION can be specified only for SELECT privilege
          case "ORA-30742": // cannot grant SELECT privilege WITH HIERARCHY OPTION on this object
          case "ORA-30990": // insufficient privileges to change owner of resource string
          case "ORA-31009": // Access denied for property string
          case "ORA-31050": // Access denied
          case "ORA-31051": // Requested access privileges not supported
          case "ORA-31086": // insufficient privileges to register schema "string"
          case "ORA-31087": // insufficient privileges to delete schema "string"
          case "ORA-31415": // change set string does not exist
          case "ORA-31466": // no publications found
          case "ORA-31534": // Change Data Capture string publisher string is missing DBA role
          case "ORA-31539": // no privilege to create job
          case "ORA-31631": // privileges are required
          case "ORA-31653": // unable to determine job operation for privilege check
          case "ORA-31685": // Object type <varname>string</varname> failed due to insufficient privileges.
                            // Failing sql is:\n<varname>string</varname>
          case "ORA-31703": // cannot grant string privilege on behalf of other users
          case "ORA-31704": // cannot revoke string privilege on behalf of other users
          case "ORA-31706": // role string not granted commonly or does not exist
          case "ORA-31707": // cannot grant string privilege
          case "ORA-31708": // role string not granted commonly or does not exist
          case "ORA-32519": // insufficient privileges to execute ORADEBUG command: string
          case "ORA-32835": // database user string does not exist
          case "ORA-32836": // database user string must be granted role string
          case "ORA-33088": // (XSAGINIT03) You do not have read permission for AGGMAP object workspace
                            // object.
          case "ORA-33090": // (XSAGINIT05) You must have read permission for workspace object to use AGGMAP
                            // workspace object.
          case "ORA-33262": // (DBERR01) Analytic workspace string does not exist, or you do not have
                            // sufficient privileges to access it.
          case "ORA-33292": // (DBERR18) Insufficient permissions to access analytic workspace string using
                            // the specified access mode.
          case "ORA-33316": // (DELDENT03) You cannot delete workspace object because you do not have PERMIT
                            // permission for it or it is readonly.
          case "ORA-34060": // (MSEXECUT10) You do not have permission to maintain workspace object.
          case "ORA-34194": // (MXCHGDCL10) You cannot change workspace object to a composite while there
                            // are permissions applied to it.
          case "ORA-34370": // (MXDSS21) Permission to attach analytic workspace string denied by a PERMIT
                            // program.
          case "ORA-34574": // (MXRENAME08) You cannot change workspace object to an unnamed composite
                            // because it has permissions attached.
          case "ORA-34854": // (PMTSETUP00) Permission expression for workspace object is invalid and will
                            // be treated as FALSE.
          case "ORA-34856": // (PMTSETUP01) Permission for workspace object does not have proper
                            // dimensionality of the BY expression.
          case "ORA-34858": // (PMTUPDAT01) Permission expression for workspace object is invalid and will
                            // not be stored.
          case "ORA-34859": // (PMTUPDAT07) Permission condition for workspace object contains an expression
                            // longer than number bytes and cannot be stored.
          case "ORA-34862": // (PMTUPDAT03) workspace object is not a dimension, so you cannot give it
                            // MAINTAIN permission.
          case "ORA-34864": // (PMTUPDAT04) A BY clause cannot be given with string permission for workspace
                            // object.
          case "ORA-34866": // (PMTUPDAT05) You cannot apply permissions to dimension composite workspace
                            // object. Instead, you can apply permissions to its base dimensions.
          case "ORA-34868": // (PMTUPDAT06) You cannot apply permissions to dimension alias workspace
                            // object. Instead, you can apply permissions to the aliased dimension.
          case "ORA-34870": // (PERMIT01) You do not have permission to read this value of workspace object.
          case "ORA-34872": // (PERMIT02) You do not have permission to write this value of workspace
                            // object.
          case "ORA-34874": // (PERMIT03) You do not have permission to run workspace object.
          case "ORA-34876": // (PERMIT04) You do not have permission to read AGGMAP workspace object.
          case "ORA-34877": // (PERMIT05) You cannot apply permissions to concat dimension workspace object.
                            // Instead, you may apply permissions to its leaf dimension(s).
          case "ORA-35581": // (XSRWLD11) You do not have permission to write this value of the target.
          case "ORA-36180": // (XSAGGR08) AGGREGATE cannot function because there is a permission clause
                            // associated with variable workspace object.
          case "ORA-36184": // (XSAGGR10) You do not have sufficient permissions for the variable workspace
                            // object.
          case "ORA-36718": // (XSALLOC00) You do not have the necessary permissions to use AGGMAP workspace
                            // object.
          case "ORA-36782": // (IOSEC02) Directory Alias used in string does not exist, or you do not have
                            // sufficient privileges to access it.
          case "ORA-36783": // Directory alias used in string does not exist, or you do not have sufficient
                            // privileges to access it.
          case "ORA-36788": // (IOSEC05) Access to the directory specified by string denied.
          case "ORA-36790": // (IOSEC06) Access to the file string denied.
          case "ORA-37161": // invalid privilege specified for OLAP object
          case "ORA-37574": // error logging to table "string.string"
          case "ORA-38171": // Insufficient privileges for SQL management object operation
          case "ORA-38330": // insufficient privilege for ILM operation
          case "ORA-38335": // sysdba privilege is required
          case "ORA-38347": // join group cannot be created
          case "ORA-38348": // join group cannot be dropped
          case "ORA-38349": // join group cannot be altered
          case "ORA-38449": // table "string" does not exist or is not accessible
          case "ORA-38455": // Expression Filter index should be created by the owner.
          case "ORA-38465": // failed to create the privilege checking trigger due to: string
          case "ORA-38466": // user does not have privileges to CREATE/MODIFY expressions
          case "ORA-38467": // user cannot GRANT/REVOKE privileges to/from himself
          case "ORA-38468": // column "string" is not identified as a column storing expressions.
          case "ORA-38469": // invalid privilege for an expression set: string
          case "ORA-38470": // cannot revoke a privilege that was not granted.
          case "ORA-38802": // edition does not exist
          case "ORA-38817": // Insufficient privileges
          case "ORA-39109": // Unprivileged users may not operate upon other users' schemas
          case "ORA-39122": // Unprivileged users may not perform string remappings.
          case "ORA-39138": // Insufficient privileges to load data not in your schema
          case "ORA-39149": // cannot link privileged user to non-privileged user
          case "ORA-39154": // Objects from foreign schemas have been removed from import
          case "ORA-39161": // Full database jobs require privileges
          case "ORA-39162": // Transportable tablespace job require privileges
          case "ORA-39166": // Object string was not found or could not be exported or imported.
          case "ORA-39181": // Only partial table data may be exported due to fine grain access control on
                            // string
          case "ORA-39209": // Parameter string requires privileges.
          case "ORA-39230": // Service name string is not available
          case "ORA-39232": // invalid remap function: string
          case "ORA-39384": // Warning: User string has been locked and the password expired.
          case "ORA-39704": // permission to modify component registry entry denied
          case "ORA-40176": // The current user does not have privilege to register or drop an algorithm.
          case "ORA-40361": // only SELECT and ALTER are valid for mining models
          case "ORA-40362": // invalid object string.string specified in the statement
          case "ORA-40363": // GRANT failed: user SYS always has SYSOPER and SYSDBA
          case "ORA-40366": // Administrative privilege cannot be granted to this user.
          case "ORA-40367": // An Administrative user cannot be altered to have no authentication type.
          case "ORA-41103": // Only the designated Cluster Director: string can undesignate itself.
          case "ORA-41301": // failed to write data to file
          case "ORA-41302": // failed to read data from file
          case "ORA-41305": // Failed to open file
          case "ORA-41602": // insufficient privileges
          case "ORA-41603": // invalid privilege type
          case "ORA-41604": // cannot revoke a privilege that was not granted
          case "ORA-41680": // Rules Manager background process string does not exist
          case "ORA-41682": // invalid rule class package
          case "ORA-41722": // insufficient privileges for database change notification
          case "ORA-42904": // full table access is restricted by Oracle Label Security
          case "ORA-44320": // cannot modify global service
          case "ORA-44401": // turn off DBA privilege
          case "ORA-44402": // restore DBA privilege
          case "ORA-44415": // Invalid ACL: Undefined privileges
          case "ORA-44819": // Execution of this WLM function is denied
          case "ORA-45502": // Insufficient privileges to perform this operation.
          case "ORA-46011": // The value of the "selectPrivilege" element is too long.
          case "ORA-46012": // The value of the "privilege" element is too long.
          case "ORA-46070": // Insufficient privileges
          case "ORA-46086": // Cannot detach from a direct-login XS session
          case "ORA-46093": // Cannot destroy a direct-login XS session
          case "ORA-46101": // Circular definition for aggregate privilege string in security class string
          case "ORA-46102": // Privilege string aggregated in security class string is not found
          case "ORA-46107": // Privilege string not found in the associated security classes
          case "ORA-46109": // Duplicate definition for privilege string in security class string.
          case "ORA-46218": // Unauthorized proxy user
          case "ORA-46231": // Granted role is not a regular role.
          case "ORA-46242": // Granting a role to XSGUEST is not allowed.
          case "ORA-46246": // session operation failed
          case "ORA-46355": // missing or invalid privilege audit option.
          case "ORA-46364": // Audit directory 'string' could not be created.
          case "ORA-46365": // Audit file 'string' could not be created.
          case "ORA-46377": // Auditing cannot be configured on the specified system privilege.
          case "ORA-46380": // invalid usage of EXCEPT clause
          case "ORA-46382": // must disable unified audit policy on this role first
          case "ORA-46383": // incorrect order of unified audit options
          case "ORA-46632": // password-based keystore does not exist
          case "ORA-46634": // creation of a local auto login keystore failed
          case "ORA-46635": // creation of an auto login keystore failed
          case "ORA-46644": // creation or open of file to store the exported keys failed
          case "ORA-46645": // open of file from which keys are to be imported failed
          case "ORA-46900": // unsupported proxy authentication
          case "ORA-46951": // unsupported format for password file 'string'
          case "ORA-46953": // The password file is not in the 12.2 format.
          case "ORA-46981": // Access to service string from string was denied.
          case "ORA-47260": // Realm Authorization to string for Realm string already defined
          case "ORA-47261": // Realm Authorization to string for Realm string not found
          case "ORA-47262": // error creating Realm Authorization to string for Realm string, string
          case "ORA-47263": // error deleting Realm Authorization to string for Realm string, string
          case "ORA-47264": // error updating Realm Authorization to string for Realm string, string
          case "ORA-47305": // Rule Set violation on string (string)
          case "ORA-47349": // error occurred when executing the rule set handler
          case "ORA-47400": // Command Rule violation for string on string
          case "ORA-47401": // Realm violation for string on string.string
          case "ORA-47408": // Realm violation for the EXECUTE command
          case "ORA-47410": // Realm violation for string on string
          case "ORA-47501": // Database Vault has already been configured.
          case "ORA-47802": // Oracle Data Pump authorization for Oracle Database Vault to string to execute
                            // action string is not found.
          case "ORA-47803": // Oracle Data Pump authorization for Oracle Database Vault to string on schema
                            // string to execute action string is not found.
          case "ORA-47804": // Oracle Database Vault application protection exception for string already
                            // exists.
          case "ORA-47920": // Authorization failed for user string to perform this operation
          case "ORA-47930": // Privilege capture string already exists.
          case "ORA-47931": // Privilege capture string does not exist.
          case "ORA-47932": // Privilege capture string is still enabled.
          case "ORA-47933": // Privilege capture string is already enabled.
          case "ORA-47934": // Two privilege captures are already enabled.
          case "ORA-47935": // Another privilege capture is enabled.
          case "ORA-47936": // Privilege capture string is already disabled.
          case "ORA-47937": // Input string does not match the given privilege capture type.
          case "ORA-47938": // GENERATE_RESULT is already running for privilege capture string.
          case "ORA-47939": // Capture context namespace or attribute does not exist or is invalid.
          case "ORA-47941": // Privilege capture string has not been enabled with run name string.
          case "ORA-47942": // Privilege capture string had been enabled with run name string.
          case "ORA-47943": // Privilege capture string is enabled with run name string.
          case "ORA-47944": // Privilege capture string has not been enabled.
          case "ORA-47955": // Oracle Data Pump authorization for Oracle Database Vault to string is not
                            // found
          case "ORA-47956": // Oracle Data Pump authorization for Oracle Database Vault to string on schema
                            // string is not found
          case "ORA-47957": // Oracle Data Pump authorization for Oracle Database Vault to string on object
                            // string.string is not found
          case "ORA-47960": // Oracle transportable tablespace authorization for Oracle Database Vault to
                            // string on tablespace string is not found.
          case "ORA-47963": // Oracle Scheduler Job authorization for Oracle Database Vault to string is not
                            // found
          case "ORA-47964": // Oracle Scheduler job authorization for Oracle Database Vault to string on
                            // schema string is not found
          case "ORA-47966": // DEBUG_CONNECT authorization for Oracle Database Vault to %0!s on schema %1!s
                            // is not found.
          case "ORA-47968": // DBCAPTURE authorization for Oracle Database Vault to string is not found.
          case "ORA-47970": // DBREPLAY authorization for Oracle Database Vault to string is not found.
          case "ORA-47972": // Oracle proxy authorization for Oracle Database Vault to string on schema
                            // string is not found.
          case "ORA-47974": // Oracle DDL authorization for Oracle Database Vault to string on schema string
                            // is not found.
          case "ORA-47976": // PREPROCESSOR authorization for Oracle Database Vault to string is not found
          case "ORA-47980": // Maintenance authorizationstring for Oracle Database Vault to stringstring is
                            // not found.
          case "ORA-47981": // Maintenance authorizationstring for Oracle Database Vault to stringstring on
                            // schema string is not found.
          case "ORA-47982": // Maintenance authorizationstring for Oracle Database Vault to string on object
                            // string.string is not found.
          case "ORA-47984": // Diagnostic authorization for Oracle Database Vault to string is not found.
          case "ORA-48108": // invalid value given for the diagnostic_dest init.ora parameter
          case "ORA-48142": // invalid permissions input for change permissions
          case "ORA-48146": // missing read, write, or exec permission on directory during ADR
                            // initialization [string] [string]
          case "ORA-48165": // user missing read, write, or exec permission on specified ADR Base directory
                            // [string]
          case "ORA-48188": // user missing read, write, or exec permission on specified directory
          case "ORA-48191": // user missing read or write permission on specified file
          case "ORA-48458": // "show incident" failed due to the following errors
          case "ORA-49802": // missing read, write, or execute permission on specified ADR home directory
                            // [string]
          case "ORA-51227": // <varname>string</varname> <varname>string</varname> does not have correct
                            // access permissions
          case "ORA-51277": // <varname>string</varname> <varname>string</varname> cannot be accessed
                            // because file system does not have correct access permissions
          case "ORA-51296": // Change access permissions for file <varname>string</varname>
          case "ORA-51298": // Ensure that file <varname>string</varname> has correct access permissions and
                            // is not locked by another process
          case "ORA-54044": // Visibility of a column having a column-level object privilege cannot be
                            // changed.
          case "ORA-54045": // An invisible column cannot be granted a column-level object privilege.
          case "ORA-54613": // INIT: internal error creating DML trigger
          case "ORA-55152": // WCS temporary tables do not exist.
          case "ORA-55153": // WCS publish coverage failed: string
          case "ORA-55154": // Request to grant or revoke privileges failed: string
          case "ORA-55302": // insufficient privileges string
          case "ORA-55336": // insufficient privileges for using one or more of the models and rules indexes
          case "ORA-55352": // insufficient privileges for policy administration
          case "ORA-55357": // insufficient privileges for the current operation
          case "ORA-55359": // unauthorized operation with policy string - string
          case "ORA-55441": // insufficient privileges to drop virtual model string
          case "ORA-55463": // missing privileges for MDSYS schema for OLS-enabled entailment
          case "ORA-55611": // No privilege to manage default Flashback Archive
          case "ORA-55612": // No privilege to manage Flashback Archive
          case "ORA-55618": // Insufficient privilege to grant Flashback Archive privilege
          case "ORA-55619": // Invalid privilege to grant on Flashback Archive
          case "ORA-55620": // No privilege to use Flashback Archive
          case "ORA-55621": // User quota on tablespace "string" is not enough for Flashback Archive
          case "ORA-55622": // DML, ALTER and CREATE UNIQUE INDEX operations are not allowed on table
                            // "string"."string"
          case "ORA-55625": // Cannot grant Flashback Archive privilege to a role
          case "ORA-55640": // Insufficient privilege to perform the Flashback Data Archive operation
          case "ORA-56607": // DRCP: Connection is already authenticated
          case "ORA-56713": // Insufficient Resource Manager privileges
          case "ORA-64491": // cannot grant XML schemas to public
          case "ORA-65029": // a Local User may not grant or revoke a Common Privilege or Role
          case "ORA-65030": // cannot grant a privilege commonly to a local user or role
          case "ORA-65031": // one may not revoke a Common Privilege from a Local User or Role
          case "ORA-65032": // a Local Role may only be granted or revoked within the current Container
          case "ORA-65033": // a common privilege may not be granted or revoked on a local object
          case "ORA-65037": // a common privilege may not be granted or revoked on a local user
          case "ORA-65056": // CONTAINER_DATA attribute is not used in a pluggable database.
          case "ORA-65092": // system privilege granted with a different scope to 'string'
          case "ORA-65175": // cannot grant SYSDBA privilege locally in the root
          case "ORA-65253": // operation not allowed within a proxy pluggable database as SYSDBA or SYSOPER
          case "ORA-65299": // cannot grant or revoke a privilege to or from multiple grantees
          case "ORA-65313": // cannot synchronize application in a proxy pluggable database without being in
                            // a SYSDBA session
          case "ORA-65317": // cannot modify the user or role created by another application
            return new PermissionDeniedException().withCause(e);

          case "ORA-00018": // maximum number of sessions exceeded
          case "ORA-00019": // maximum number of session licenses exceeded
          case "ORA-00020": // maximum number of processes (string) exceeded
          case "ORA-00021": // session attached to some other process; cannot switch session
          case "ORA-00024": // logins from more than one process not allowed in single-process mode
          case "ORA-00025": // failed to allocate string
          case "ORA-00028": // your session has been killed
          case "ORA-00029": // session is not a user session
          case "ORA-00030": // User session ID does not exist.
          case "ORA-00031": // session marked for kill
          case "ORA-00050": // operating system error occurred while obtaining an enqueue
          case "ORA-00051": // timeout occurred while waiting for a resource
          case "ORA-00052": // maximum number of enqueue resources (string) exceeded
          case "ORA-00053": // maximum number of enqueues exceeded
          case "ORA-00054": // resource busy and acquire with NOWAIT specified or timeout expired
          case "ORA-00055": // maximum number of DML locks exceeded
          case "ORA-00056": // DDL lock on object 'string.string' is already held in an incompatible mode
          case "ORA-00057": // maximum number of temporary table locks exceeded
          case "ORA-00058": // DB_BLOCK_SIZE must be string to mount this database (not string)
          case "ORA-00059": // maximum number of DB_FILES exceeded
          case "ORA-00060": // deadlock detected while waiting for resource
          case "ORA-00061": // another instance has a different DML_LOCKS setting
          case "ORA-00062": // DML full-table lock cannot be acquired; DML_LOCKS is 0
          case "ORA-00063": // maximum number of log files exceeded string
          case "ORA-00064": // object is too large to allocate on this O/S (string,string,string)
          case "ORA-00065": // initialization of FIXED_DATE failed
          case "ORA-00066": // SID string contains an illegal character
          case "ORA-01010": // invalid OCI operation
          case "ORA-01948": // identifier's name length (string) exceeds maximum (string)
          case "ORA-01994": // GRANT failed: password file missing or disabled
          case "ORA-02426": // privilege grant failed
          case "ORA-02427": // create view failed
          case "ORA-02726": // osnpop: access error on oracle executable
          case "ORA-02727": // osnpop: access error on orapop executable
          case "ORA-02728": // osnfop: access error on oracle executable
          case "ORA-02777": // Stat failed on log directory
          case "ORA-02778": // Name given for the log directory is invalid
          case "ORA-02779": // Stat failed on core dump directory
          case "ORA-02780": // Name given for the core dump directory is invalid
          case "ORA-06419": // NETCMN: server can not start oracle
          case "ORA-06979": // X.25 Driver: server cannot start oracle
          case "ORA-09914": // Unable to open the ORACLE password file.
          case "ORA-09926": // Unable to set effective privilege set of the server
          case "ORA-09931": // Unable to open ORACLE password file for reading
          case "ORA-12481": // effective label not within program unit clearance range
          case "ORA-12658": // ANO service required but TNS version is incompatible
          case "ORA-12678": // Authentication disabled but required
          case "ORA-12682": // Login failed: the SecurID card is in next PRN mode
          case "ORA-17500": // ODM err:string
          case "ORA-17501": // logical block size string is invalid
          case "ORA-17502": // ksfdcre:string Failed to create file string
          case "ORA-17503": // ksfdopn:string Failed to open file string
          case "ORA-17504": // ksfddel:Failed to delete file string
          case "ORA-17505": // ksfdrsz:string Failed to resize file to size string blocks
          case "ORA-17506": // I/O Error Simulation
          case "ORA-17507": // I/O request size string is not a multiple of logical block size
          case "ORA-17508": // I/O request buffer ptr is not alligned
          case "ORA-17509": // Attempt to do i/o beyond block1 offset
          case "ORA-17510": // Attempt to do i/o beyond file size
          case "ORA-17512": // Block Verification Failed
          case "ORA-17610": // file 'string' does not exist and no size specified
          case "ORA-17611": // ksfd: file 'string' cannot be accessed, global open closed
          case "ORA-17612": // Failed to discover Oracle Disk Manager library, return value string
          case "ORA-17613": // Failed to initialize Oracle Disk Manager library: string
          case "ORA-17618": // Unable to update block 0 to version 10 format
          case "ORA-17619": // max number of processes using I/O slaves in a instance reached
          case "ORA-17620": // failed to register the network adapter with Oracle Disk Manager library:
                            // string
          case "ORA-17621": // failed to register the memory with Oracle Disk Manager library
          case "ORA-17622": // failed to deregister the memory with Oracle Disk Manager library
          case "ORA-17624": // Failed to delete directory string
          case "ORA-17626": // ksfdcre: string file exists
          case "ORA-17627": // string
          case "ORA-17628": // Oracle error string returned by remote Oracle server
          case "ORA-17629": // Cannot connect to the remote database server
          case "ORA-24327": // need explicit attach before authenticating a user
          case "ORA-25277": // cannot grant or revoke object privilege on release 8.0 compatible queues
          case "ORA-27040": // file create error, unable to create file
          case "ORA-27056": // could not delete file
          case "ORA-27057": // cannot perform async I/O to file
          case "ORA-27058": // file I/O question parameter is invalid
          case "ORA-27059": // could not reduce file size
          case "ORA-27060": // could not set close-on-exec bit on file
          case "ORA-27061": // waiting for async I/Os failed
          case "ORA-27062": // could not find pending async I/Os
          case "ORA-27063": // number of bytes read/written is incorrect
          case "ORA-27064": // cannot perform async I/O to file
          case "ORA-27065": // cannot perform async vector I/O to file
          case "ORA-27066": // number of buffers in vector I/O exceeds maximum
          case "ORA-27067": // size of I/O buffer is invalid
          case "ORA-27068": // I/O buffer is not aligned properly
          case "ORA-27069": // attempt to do I/O beyond the range of the file
          case "ORA-27070": // async read/write failed
          case "ORA-27071": // unable to seek to desired position in file
          case "ORA-27072": // File I/O error
          case "ORA-27073": // Trying to close a file which has async I/Os pending to be dequeued
          case "ORA-27074": // unable to determine limit for open files
          case "ORA-27075": // SSTMOFRC constant too large
          case "ORA-27076": // unable to set limit for open files
          case "ORA-27077": // too many files open
          case "ORA-27078": // unable to determine limit for open files
          case "ORA-27079": // unable to set async IO limit
          case "ORA-27080": // too many files open
          case "ORA-27081": // unable to close the file
          case "ORA-27083": // waiting for async I/Os failed
          case "ORA-27084": // unable to get/set file status flags
          case "ORA-27086": // unable to lock file - already in use
          case "ORA-27087": // unable to get share lock - file not readable
          case "ORA-27088": // unable to get file status
          case "ORA-27089": // unable to release advisory lock
          case "ORA-27090": // Unable to reserve kernel resources for asynchronous disk I/O
          case "ORA-27091": // unable to queue I/O
          case "ORA-27092": // size of file exceeds file size limit of the process
          case "ORA-27093": // could not delete directory
          case "ORA-27094": // raw volume used can damage partition table
          case "ORA-27100": // shared memory realm already exists
          case "ORA-27101": // shared memory realm does not exist
          case "ORA-27102": // out of memory
          case "ORA-27103": // internal error
          case "ORA-27120": // unable to removed shared memory segment
          case "ORA-27121": // unable to determine size of shared memory segment
          case "ORA-27122": // unable to protect memory
          case "ORA-27123": // unable to attach to shared memory segment
          case "ORA-27124": // unable to detach from shared memory segment
          case "ORA-27125": // unable to create shared memory segment
          case "ORA-27126": // unable to lock shared memory segment in core
          case "ORA-27127": // unable to unlock shared memory segment
          case "ORA-27128": // unable to determine pagesize
          case "ORA-27140": // attach to post/wait facility failed
          case "ORA-27141": // invalid process ID
          case "ORA-27142": // could not create new process
          case "ORA-27143": // OS system call failure
          case "ORA-27144": // attempt to kill process failed
          case "ORA-27145": // insufficient resources for requested number of processes
          case "ORA-27146": // post/wait initialization failed
          case "ORA-27147": // post/wait reset failed
          case "ORA-27148": // spawn wait error
          case "ORA-27149": // assignment out of range
          case "ORA-27150": // attempt to notify process of pending oradebug call failed
          case "ORA-27151": // buffer not large enough to hold process ID string
          case "ORA-27152": // attempt to post process failed
          case "ORA-27153": // wait operation failed
          case "ORA-27154": // post/wait create failed
          case "ORA-27155": // could not execute file
          case "ORA-27156": // request for process information failed
          case "ORA-27157": // OS post/wait facility removed
          case "ORA-28017": // The password file is in the legacy format.
          case "ORA-28041": // Authentication protocol internal error
          case "ORA-28042": // Server authentication failed
          case "ORA-28045": // SSL authentication between database and OID failed
          case "ORA-28169": // unsupported certificate type
          case "ORA-28170": // unsupported certificate version
          case "ORA-28171": // unsupported Kerberos version
          case "ORA-28173": // certificate not provided by proxy
          case "ORA-28174": // Kerberos ticket not provided by proxy
          case "ORA-28175": // incorrect certificate type
          case "ORA-28176": // incorrect certificate version
          case "ORA-28177": // incorrect Kerberos ticket version
          case "ORA-28271": // No permission to read user entry in LDAP directory service.
          case "ORA-28300": // No permission to read user entry in LDAP directory service.
          case "ORA-28368": // cannot auto-create wallet
          case "ORA-28591": // agent control utility: unable to access parameter file
          case "ORA-29291": // file remove operation failed
          case "ORA-29292": // file rename operation failed
          case "ORA-30159": // OCIFileOpen: Cannot create the file or cannot open in the requested mode
          case "ORA-30160": // Unable to access the file
          case "ORA-30161": // A system error occurred during the OCIFile function call
          case "ORA-31460": // logfile location string is not an existing directory
          case "ORA-32024": // invalid directory specified for audit_file_dest parameter
          case "ORA-36694": // (XSRELTBL01) The value cannot be added to dimension workspace object.
          case "ORA-36767": // (XSAGGCNTMOVE05) workspace object cannot be used as an AGGCOUNT while there
                            // are permissions applied to it.
          case "ORA-38414": // invalid datatype for the attribute string
          case "ORA-38440": // attribute set string does not exist
          case "ORA-38801": // improper value for ORA_EDITION
          case "ORA-39113": // Unable to determine database version
          case "ORA-48143": // error changing permissions for a file
          case "ORA-48144": // error encounted while performing standard file I/O
          case "ORA-48152": // lock table is full
          case "ORA-48179": // OS file synchronization failure
          case "ORA-48180": // OS open system call failure
          case "ORA-48181": // OS write system call failure
          case "ORA-48182": // OS read system call failure
          case "ORA-48183": // OS close system call failure
          case "ORA-48184": // OS seek system call failure
          case "ORA-48185": // OS file size system call failure
          case "ORA-48186": // OS check file exists system call failure
          case "ORA-48187": // specified directory does not exist
          case "ORA-48189": // OS command to create directory failed
          case "ORA-48190": // OS unlink system call failure
          case "ORA-48192": // OS command to move a file failed
          case "ORA-48193": // OS command to open a directory failed
          case "ORA-48194": // OS command to close a directory failed
          case "ORA-48195": // OS command to remove a directory failed
          case "ORA-48196": // OS command to release advisory lock failed
          case "ORA-48197": // OS command to get the file status failed
          case "ORA-48198": // OS command to change the file permissions failed
          case "ORA-48199": // OS command to copy a file failed
          case "ORA-48483": // Spooling failed, it may be because the spool file cannot be created due to a
                            // permission issue
          case "ORA-49427": // No such file or file not accessible [string]
          case "ORA-49428": // No such directory or directory not accessible [string]
          case "ORA-53900": // I/O failure string.
            return new ServerException().withCause(e);

          case "ORA-00026": // missing or invalid session ID
          case "ORA-00603": // ORACLE server session terminated by fatal error
          case "ORA-03136": // inbound connection timed out
          case "ORA-06000": // NETASY: port open failure
          case "ORA-06001": // NETASY: port set-up failure
          case "ORA-06002": // NETASY: port read failure
          case "ORA-06003": // NETASY: port write failure
          case "ORA-06004": // NETASY: dialogue file open failure
          case "ORA-06005": // NETASY: dialogue file read failure
          case "ORA-06019": // NETASY: invalid login (connect) string
          case "ORA-06040": // NETDNT: invalid login (connect) string
          case "ORA-06105": // NETTCP: remote host is unknown
          case "ORA-06112": // NETTCP: invalid buffer size
          case "ORA-06114": // NETTCP: SID lookup failure
          case "ORA-06133": // NETTCP: file not found
          case "ORA-06135": // NETTCP: connection rejected; server is stopping
          case "ORA-06136": // NETTCP: error during connection handshake
          case "ORA-06137": // NETTCP: error during connection handshake
          case "ORA-06138": // NETTCP: error during connection handshake
          case "ORA-06143": // NETTCP: maximum connections exceeded
          case "ORA-06208": // TWOTASK: invalid login (connect) string
          case "ORA-06401": // NETCMN: invalid driver designator
          case "ORA-06404": // NETCMN: invalid login (connect) string
          case "ORA-06420": // NETCMN: SID lookup failure
          case "ORA-06713": // TLI Driver: error on connect
          case "ORA-06720": // TLI Driver: SID lookup failure
          case "ORA-06817": // TLI Driver: could not read the Novell network address
          case "ORA-06970": // X.25 Driver: remote host is unknown
          case "ORA-06973": // X.25 Driver: invalid buffer size
          case "ORA-06974": // X.25 Driver: SID lookup failure
          case "ORA-09843": // soacon: Archmon unable to create named pipe.
          case "ORA-09844": // soacon: Archmon unable to open named pipe.
          case "ORA-09845": // soacon: Archmon unable to open named pipe.
          case "ORA-12547": // TNS:lost contact
          case "ORA-12548": // TNS:incomplete read or write
          case "ORA-12564": // TNS:connection refused
          case "ORA-12619": // TNS:unable to grant requested service
          case "ORA-12668": // Dedicated server: outbound protocol does not support proxies
          case "ORA-12669": // Shared server: outbound protocol does not support proxies
          case "ORA-12689": // Server Authentication required, but not supported
          case "ORA-24027": // AQ HTTP propagation encountered error, status-code string, string
          case "ORA-25288": // AQ HTTP propagation encountered error, status-code number, string
          case "ORA-28028": // could not authenticate remote server
          case "ORA-28172": // distinguished name not provided by proxy
            return new ConnectionException().withCause(e);
        }
      }

      // switch(e.getErrorCode())
    }

    return null;
  }
}
