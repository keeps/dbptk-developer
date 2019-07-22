/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.structure;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.utils.ListUtils;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Miguel Coutada
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DatabaseStructure {

  private static final Logger logger = LoggerFactory.getLogger(DatabaseStructure.class);

  private String name;

  private String description;

  private String archiver;

  private String archiverContact;

  private String dataOwner;

  private String dataOriginTimespan;

  private String producerApplication;

  private DateTime archivalDate; // date of creation of archive on SIARD

  private String clientMachine;

  private String productName; // databaseProduct on SIARD

  private String productVersion;

  private String databaseUser;

  private Integer defaultTransactionIsolationLevel;

  private String extraNameCharacters;

  private String stringFunctions;

  private String systemFunctions;

  private String timeDateFunctions;

  private String url; // connection on SIARD

  private Boolean supportsANSI92EntryLevelSQL;

  private Boolean supportsANSI92IntermediateSQL;

  private Boolean supportsANSI92FullSQL;

  private Boolean supportsCoreSQLGrammar;

  private List<SchemaStructure> schemas;

  private List<UserStructure> users;

  private List<RoleStructure> roles;

  private List<PrivilegeStructure> privileges;

  /**
   * Create a new empty database. All attributes are null, except for tables,
   * which is a empty list
   */
  public DatabaseStructure() {
    name = null;
    archivalDate = DateTime.now();
    productName = null;
    productVersion = null;
    schemas = new ArrayList<SchemaStructure>();
    users = new ArrayList<UserStructure>();
    roles = new ArrayList<RoleStructure>();
    privileges = new ArrayList<PrivilegeStructure>();
  }

  /**
   * Create a new database
   *
   * @param name
   *          the database name
   * @param creationDate
   *          date when the database was created, ISO 8601 format
   * @param productName
   *          the DBMS name
   * @param productVersion
   *          the DBMS version
   * @param defaultTransactionIsolationLevel
   *          database's default transaction isolation level
   * @param extraNameCharacters
   *          "extra" characters that can be used in unquoted identifier names
   *          (those beyond a-z, A-Z, 0-9 and _)
   * @param stringFunctions
   *          comma-separated list of string functions available with this
   *          database
   * @param systemFunctions
   *          comma-separated list of system functions available with this
   *          database
   * @param timeDateFunctions
   *          comma-separated list of the time and date functions available with
   *          this database
   * @param url
   *          URL for the DBMS
   * @param supportsANSI92EntryLevelSQL
   *          whether this database supports the ANSI92 entry level SQL grammar
   * @param supportsANSI92IntermediateSQL
   *          whether this database supports the ANSI92 intermediate SQL grammar
   * @param supportsANSI92FullSQL
   *          whether this database supports the ANSI92 full SQL grammar
   * @param supportsCoreSQLGrammar
   *          whether this database supports the ODBC Core SQL grammar
   */

  // TODO complete doc
  public DatabaseStructure(String name, String description, String archiver, String archiverContact, String dataOwner,
    String dataOriginTimespan, String producerApplication, DateTime archivalDate, String clientMachine,
    String productName, String productVersion, String databaseUser, Integer defaultTransactionIsolationLevel,
    String extraNameCharacters, String stringFunctions, String systemFunctions, String timeDateFunctions, String url,
    Boolean supportsANSI92EntryLevelSQL, Boolean supportsANSI92IntermediateSQL, Boolean supportsANSI92FullSQL,
    Boolean supportsCoreSQLGrammar, List<SchemaStructure> schemas, List<UserStructure> users, List<RoleStructure> roles,
    List<PrivilegeStructure> privileges) {
    super();
    this.name = name;
    this.description = description;
    this.archivalDate = archivalDate;
    this.archiver = archiver;
    this.archiverContact = archiverContact;
    this.dataOwner = dataOwner;
    this.dataOriginTimespan = dataOriginTimespan;
    this.producerApplication = producerApplication;
    this.clientMachine = clientMachine;
    this.productName = productName;
    this.productVersion = productVersion;
    this.databaseUser = databaseUser;
    this.defaultTransactionIsolationLevel = defaultTransactionIsolationLevel;
    this.extraNameCharacters = extraNameCharacters;
    this.stringFunctions = stringFunctions;
    this.systemFunctions = systemFunctions;
    this.timeDateFunctions = timeDateFunctions;
    this.url = url;
    this.supportsANSI92EntryLevelSQL = supportsANSI92EntryLevelSQL;
    this.supportsANSI92IntermediateSQL = supportsANSI92IntermediateSQL;
    this.supportsANSI92FullSQL = supportsANSI92FullSQL;
    this.supportsCoreSQLGrammar = supportsCoreSQLGrammar;
    this.schemas = schemas;
    this.users = users;
    this.roles = roles;
    this.privileges = privileges;
  }

  /**
   * Sort the tables topologically by its foreign key references. This method is
   * useful when inserting data into the database, so the foreign key constrains
   * will be respected
   *
   * @param tables
   * @return the sorted table list or null if the tables cannot be sorted
   *         topologically (recursive graph)
   */
  public static List<TableStructure> topologicSort(List<TableStructure> tables) {

    List<TableStructure> sortedTables = new ArrayList<TableStructure>(tables.size());
    boolean canSortTopologically = true;
    while (sortedTables.size() != tables.size() && canSortTopologically) {
      List<TableStructure> filtered = filterReferencedTables(tables, sortedTables);
      if (!filtered.isEmpty()) {
        sortedTables.addAll(filtered);
      } else {
        canSortTopologically = false;
        sortedTables = null;
        logger.error("Cannot sort topologicaly");
      }
    }
    return sortedTables;
  }

  private static List<TableStructure> filterReferencedTables(List<TableStructure> allTables,
    List<TableStructure> insertedTables) {

    List<TableStructure> referencedTables = new ArrayList<TableStructure>();
    for (TableStructure table : allTables) {
      if (!insertedTables.contains(table)) {
        boolean allReferredTablesInserted = true;
        for (ForeignKey fkey : table.getForeignKeys()) {
          if (!containsTable(insertedTables, fkey.getReferencedTable())) {
            allReferredTablesInserted = false;
            break;
          }
        }
        if (allReferredTablesInserted) {
          referencedTables.add(table);
        }
      }
    }
    return referencedTables;
  }

  private static boolean containsTable(List<TableStructure> tables, String tableId) {
    boolean foundIt = false;
    for (TableStructure table : tables) {
      if (table.getId().equalsIgnoreCase(tableId)) {
        foundIt = true;
        break;
      }
    }
    return foundIt;
  }

  /**
   * @return the date when the database was archived
   */
  public DateTime getArchivalDate() {
    return archivalDate;
  }

  /**
   * @param archivalDate
   *          the date when the database was archived
   */
  public void setArchivalDate(DateTime archivalDate) {
    this.archivalDate = archivalDate;
  }

  /**
   * @return database name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name
   *          database name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return the DBMS name
   */
  public String getProductName() {
    return productName;
  }

  /**
   * @param productName
   *          the DBMS name
   */
  public void setProductName(String productName) {
    this.productName = productName;
  }

  /**
   * @return the DBMS version
   */
  public String getProductVersion() {
    return productVersion;
  }

  /**
   * @param productVersion
   *          the DBMS version
   */
  public void setProductVersion(String productVersion) {
    this.productVersion = productVersion;
  }

  /**
   * @return database's default transaction isolation level
   */
  public Integer getDefaultTransactionIsolationLevel() {
    return defaultTransactionIsolationLevel;
  }

  /**
   * @param defaultTransactionIsolationLevel
   *          database's default transaction isolation level
   */
  public void setDefaultTransactionIsolationLevel(Integer defaultTransactionIsolationLevel) {
    this.defaultTransactionIsolationLevel = defaultTransactionIsolationLevel;
  }

  /**
   * @return "extra" characters that can be used in unquoted identifier names
   *         (those beyond a-z, A-Z, 0-9 and _)
   */
  public String getExtraNameCharacters() {
    return extraNameCharacters;
  }

  /**
   * @param extraNameCharacters
   *          "extra" characters that can be used in unquoted identifier names
   *          (those beyond a-z, A-Z, 0-9 and _)
   */
  public void setExtraNameCharacters(String extraNameCharacters) {
    this.extraNameCharacters = extraNameCharacters;
  }

  /**
   * @return comma-separated list of string functions available with this database
   */
  public String getStringFunctions() {
    return stringFunctions;
  }

  /**
   * @param stringFunctions
   *          comma-separated list of string functions available with this
   *          database
   */
  public void setStringFunctions(String stringFunctions) {
    this.stringFunctions = stringFunctions;
  }

  /**
   * @return whether this database supports the ANSI92 entry level SQL grammar
   */
  public Boolean getSupportsANSI92EntryLevelSQL() {
    return supportsANSI92EntryLevelSQL;
  }

  /**
   * @param supportsANSI92EntryLevelSQL
   *          whether this database supports the ANSI92 entry level SQL grammar
   */
  public void setSupportsANSI92EntryLevelSQL(Boolean supportsANSI92EntryLevelSQL) {
    this.supportsANSI92EntryLevelSQL = supportsANSI92EntryLevelSQL;
  }

  /**
   * @return whether this database supports the ANSI92 full SQL grammar
   */
  public Boolean getSupportsANSI92FullSQL() {
    return supportsANSI92FullSQL;
  }

  /**
   * @param supportsANSI92FullSQL
   *          whether this database supports the ANSI92 full SQL grammar
   */
  public void setSupportsANSI92FullSQL(Boolean supportsANSI92FullSQL) {
    this.supportsANSI92FullSQL = supportsANSI92FullSQL;
  }

  /**
   * @return whether this database supports the ANSI92 intermediate SQL grammar
   */
  public Boolean getSupportsANSI92IntermediateSQL() {
    return supportsANSI92IntermediateSQL;
  }

  /**
   * @param supportsANSI92IntermediateSQL
   *          whether this database supports the ANSI92 intermediate SQL grammar
   */
  public void setSupportsANSI92IntermediateSQL(Boolean supportsANSI92IntermediateSQL) {
    this.supportsANSI92IntermediateSQL = supportsANSI92IntermediateSQL;
  }

  /**
   * @return whether this database supports the ODBC Core SQL grammar
   */
  public Boolean getSupportsCoreSQLGrammar() {
    return supportsCoreSQLGrammar;
  }

  /**
   * @param supportsCoreSQLGrammar
   *          whether this database supports the ODBC Core SQL grammar
   */
  public void setSupportsCoreSQLGrammar(Boolean supportsCoreSQLGrammar) {
    this.supportsCoreSQLGrammar = supportsCoreSQLGrammar;
  }

  /**
   * @return comma-separated list of systemfunctions available with this database
   */
  public String getSystemFunctions() {
    return systemFunctions;
  }

  /**
   * @param systemFunctions
   *          comma-separated list of system functions available with this
   *          database
   */
  public void setSystemFunctions(String systemFunctions) {
    this.systemFunctions = systemFunctions;
  }

  /**
   * @return comma-separated list of the time and date functions available with
   *         this database
   */
  public String getTimeDateFunctions() {
    return timeDateFunctions;
  }

  /**
   * @param timeDateFunctions
   *          comma-separated list of the time and date functions available with
   *          this database
   */
  public void setTimeDateFunctions(String timeDateFunctions) {
    this.timeDateFunctions = timeDateFunctions;
  }

  /**
   * @return URL for the DBMS
   */
  public String getUrl() {
    return url;
  }

  /**
   * @param url
   *          URL for the DBMS
   */
  public void setUrl(String url) {
    this.url = url;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getArchiver() {
    return archiver;
  }

  public void setArchiver(String archiver) {
    this.archiver = archiver;
  }

  public String getArchiverContact() {
    return archiverContact;
  }

  public void setArchiverContact(String archiverContact) {
    this.archiverContact = archiverContact;
  }

  public String getDataOwner() {
    return dataOwner;
  }

  public void setDataOwner(String dataOwner) {
    this.dataOwner = dataOwner;
  }

  public String getDataOriginTimespan() {
    return dataOriginTimespan;
  }

  public void setDataOriginTimespan(String dataOriginTimespan) {
    this.dataOriginTimespan = dataOriginTimespan;
  }

  public String getProducerApplication() {
    return producerApplication;
  }

  public void setProducerApplication(String producerApplication) {
    this.producerApplication = producerApplication;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public void setClientMachine(String clientMachine) {
    this.clientMachine = clientMachine;
  }

  public String getDatabaseUser() {
    return databaseUser;
  }

  public void setDatabaseUser(String databaseUser) {
    this.databaseUser = databaseUser;
  }

  /**
   * @return this database tables
   */
  public List<SchemaStructure> getSchemas() {
    return schemas;
  }

  /**
   * @param schemas
   *          this database schemas
   */
  public void setSchemas(List<SchemaStructure> schemas) {
    this.schemas = schemas;
  }

  public List<UserStructure> getUsers() {
    return users;
  }

  public void setUsers(List<UserStructure> users) {
    this.users = users;
  }

  public List<RoleStructure> getRoles() {
    return roles;
  }

  public void setRoles(List<RoleStructure> roles) {
    this.roles = roles;
  }

  public List<PrivilegeStructure> getPrivileges() {
    return privileges;
  }

  public PrivilegeStructure getPrivilege(PrivilegeStructure privilege) {
    for (PrivilegeStructure privilegeStructure : getPrivileges()) {
      if (privilegeStructure.getType().equals(privilege.getType())
      && privilegeStructure.getObject().equals(privilege.getObject())
      && privilegeStructure.getGrantor().equals(privilege.getGrantor())
      && privilegeStructure.getGrantee().equals(privilege.getGrantee())) {
        return privilegeStructure;
      }
    }

    return null;
  }


  public void setPrivileges(List<PrivilegeStructure> privileges) {
    this.privileges = privileges;
  }

  /**
   * Lookup a table structure by its table id
   *
   * @param tableId
   *          the table id
   * @return the table structure
   */
  public TableStructure getTableById(String tableId) {
    TableStructure ret = null;
    for (SchemaStructure schema : getSchemas()) {
      ret = schema.getTableById(tableId);
      if (ret != null) {
        break;
      }
    }
    return ret;
  }

  public SchemaStructure getSchemaByName(String schemaName) {
    for (SchemaStructure schema : schemas) {
      if (schema.getName().equalsIgnoreCase(schemaName)) {
        return schema;
      }
    }
    return null;
  }

  public UserStructure getUserByName(String userName) {
    for (UserStructure user : users) {
      if (user.getName().equalsIgnoreCase(userName)) {
        return user;
      }
    }
    return null;
  }

  public RoleStructure getRoleByName(String userName) {
    for (RoleStructure role : roles) {
      if (role.getName().equalsIgnoreCase(userName)) {
        return role;
      }
    }
    return null;
  }

  public void updateSchemaDescription(String schemaName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);
    schema.setDescription(description);
  }

  public void updateTableDescription(String schemaName, String tableName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);
    if (schema.getName().equalsIgnoreCase(schemaName)) {
      schema.getTableByName(tableName).setDescription(description);
    }
  }

  public void updateTableColumnDescription(String schemaName, String tableName, String columnName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);
    for (ColumnStructure column : schema.getTableByName(tableName).getColumns()) {
      if (column.getName().equalsIgnoreCase(columnName)) {
        column.setDescription(description);
      }
    }
  }

  public void updateTriggerDescription(String schemaName, String tableName, String triggerName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (Trigger trigger : schema.getTableByName(tableName).getTriggers()) {
      if (trigger.getName().equalsIgnoreCase(triggerName)) {
        trigger.setDescription(description);
      }
    }
  }

  public void updatePrimaryKeyDescription(String schemaName, String tableName, String primaryKeyName,
    String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    PrimaryKey primaryKey = schema.getTableByName(tableName).getPrimaryKey();
    if (primaryKey.getName().equalsIgnoreCase(primaryKeyName)) {
      primaryKey.setDescription(description);
    }
  }

  public void updateForeignKeyDescription(String schemaName, String tableName, String foreignKeyName,
    String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (ForeignKey foreignKey : schema.getTableByName(tableName).getForeignKeys()) {
      if (foreignKey.getName().equalsIgnoreCase(foreignKeyName)) {
        foreignKey.setDescription(description);
      }
    }
  }

  public void updateCandidateKeyDescription(String schemaName, String tableName, String candidateKeyName,
    String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (CandidateKey candidateKey : schema.getTableByName(tableName).getCandidateKeys()) {
      if (candidateKey.getName().equalsIgnoreCase(candidateKeyName)) {
        candidateKey.setDescription(description);
      }
    }
  }

  public void updateCheckConstraintDescription(String schemaName, String tableName, String constraintName,
    String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (CheckConstraint checkConstraint : schema.getTableByName(tableName).getCheckConstraints()) {
      if (checkConstraint.getName().equalsIgnoreCase(constraintName)) {
        checkConstraint.setDescription(description);
      }
    }
  }

  public void updateViewDescription(String schemaName, String viewName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);
    schema.getViewByName(viewName).setDescription(description);
  }

  public void updateViewColumnDescription(String schemaName, String viewName, String columnName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (ColumnStructure column : schema.getViewByName(viewName).getColumns()) {
      if (column.getName().equalsIgnoreCase(columnName)) {
        column.setDescription(description);
      }
    }
  }

  public void updateRoutineDescription(String schemaName, String routineName, String description) {
    SchemaStructure schema = getSchemaByName(schemaName);
    schema.getRoutineByName(routineName).setDescription(description);
  }

  public void updateRoutineParameterDescription(String schemaName, String routineName, String parameterName,
    String description) {
    SchemaStructure schema = getSchemaByName(schemaName);

    for (Parameter parameter : schema.getRoutineByName(routineName).getParameters()) {
      if (parameter.getName().equalsIgnoreCase(parameterName)) {
        parameter.setDescription(description);
      }
    }
  }

  public void updateUserDescription(String userName, String description) {
    UserStructure user = getUserByName(userName);
    user.setDescription(description);
  }

  public void updateRoleDescription(String roleName, String description) {
    RoleStructure role = getRoleByName(roleName);
    role.setDescription(description);
  }

  public void updatePrivilegeDescription(PrivilegeStructure privilege, String description) {
    for (PrivilegeStructure p : getPrivileges()) {
      if (p.getType().equals(privilege.getType())
          && p.getObject().equals(privilege.getObject())
          && p.getGrantor().equals(privilege.getGrantor())
          && p.getGrantee().equals(privilege.getGrantee())) {
        p.setDescription(description);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DatabaseStructure [name=");
    builder.append(name);
    builder.append("\n");
    builder.append("archivalDate=");
    builder.append(archivalDate);
    builder.append("\n");
    builder.append("productName=");
    builder.append(productName);
    builder.append("\n");
    builder.append("productVersion=");
    builder.append(productVersion);
    builder.append("\n");
    builder.append("defaultTransactionIsolationLevel=");
    builder.append(defaultTransactionIsolationLevel);
    builder.append("\n");
    builder.append("extraNameCharacters=");
    builder.append(extraNameCharacters);
    builder.append("\n");
    builder.append("stringFunctions=");
    builder.append(stringFunctions);
    builder.append("\n");
    builder.append("systemFunctions=");
    builder.append(systemFunctions);
    builder.append("\n");
    builder.append("timeDateFunctions=");
    builder.append(timeDateFunctions);
    builder.append("\n");
    builder.append("url=");
    builder.append(url);
    builder.append("\n");
    builder.append("supportsANSI92EntryLevelSQL=");
    builder.append(supportsANSI92EntryLevelSQL);
    builder.append("\n");
    builder.append("supportsANSI92IntermediateSQL=");
    builder.append(supportsANSI92IntermediateSQL);
    builder.append("\n");
    builder.append("supportsANSI92FullSQL=");
    builder.append(supportsANSI92FullSQL);
    builder.append("\n");
    builder.append("supportsCoreSQLGrammar=");
    builder.append(supportsCoreSQLGrammar);
    builder.append("\n");
    builder.append("schemas=");
    for (SchemaStructure schema : schemas) {
      builder.append(schema.toString());
    }
    builder.append("\n------ END SCHEMAS ------");
    builder.append("\n****** END STRUCTURE ******");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((archivalDate == null) ? 0 : archivalDate.hashCode());
    result = prime * result + ((archiver == null) ? 0 : archiver.hashCode());
    result = prime * result + ((archiverContact == null) ? 0 : archiverContact.hashCode());
    result = prime * result + ((clientMachine == null) ? 0 : clientMachine.hashCode());
    result = prime * result + ((dataOriginTimespan == null) ? 0 : dataOriginTimespan.hashCode());
    result = prime * result + ((dataOwner == null) ? 0 : dataOwner.hashCode());
    result = prime * result + ((databaseUser == null) ? 0 : databaseUser.hashCode());
    result = prime * result
      + ((defaultTransactionIsolationLevel == null) ? 0 : defaultTransactionIsolationLevel.hashCode());
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((extraNameCharacters == null) ? 0 : extraNameCharacters.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((privileges == null) ? 0 : privileges.hashCode());
    result = prime * result + ((producerApplication == null) ? 0 : producerApplication.hashCode());
    result = prime * result + ((productName == null) ? 0 : productName.hashCode());
    result = prime * result + ((productVersion == null) ? 0 : productVersion.hashCode());
    result = prime * result + ((roles == null) ? 0 : roles.hashCode());
    result = prime * result + ((schemas == null) ? 0 : schemas.hashCode());
    result = prime * result + ((stringFunctions == null) ? 0 : stringFunctions.hashCode());
    result = prime * result + ((supportsANSI92EntryLevelSQL == null) ? 0 : supportsANSI92EntryLevelSQL.hashCode());
    result = prime * result + ((supportsANSI92FullSQL == null) ? 0 : supportsANSI92FullSQL.hashCode());
    result = prime * result + ((supportsANSI92IntermediateSQL == null) ? 0 : supportsANSI92IntermediateSQL.hashCode());
    result = prime * result + ((supportsCoreSQLGrammar == null) ? 0 : supportsCoreSQLGrammar.hashCode());
    result = prime * result + ((systemFunctions == null) ? 0 : systemFunctions.hashCode());
    result = prime * result + ((timeDateFunctions == null) ? 0 : timeDateFunctions.hashCode());
    result = prime * result + ((url == null) ? 0 : url.hashCode());
    result = prime * result + ((users == null) ? 0 : users.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    DatabaseStructure other = (DatabaseStructure) obj;
    if (archivalDate == null) {
      if (other.archivalDate != null) {
        return false;
      }
    } else if (!archivalDate.equals(other.archivalDate)) {
      return false;
    }
    if (archiver == null) {
      if (other.archiver != null) {
        return false;
      }
    } else if (!archiver.equals(other.archiver)) {
      return false;
    }
    if (archiverContact == null) {
      if (other.archiverContact != null) {
        return false;
      }
    } else if (!archiverContact.equals(other.archiverContact)) {
      return false;
    }
    if (clientMachine == null) {
      if (other.clientMachine != null) {
        return false;
      }
    } else if (!clientMachine.equals(other.clientMachine)) {
      return false;
    }
    if (dataOriginTimespan == null) {
      if (other.dataOriginTimespan != null) {
        return false;
      }
    } else if (!dataOriginTimespan.equals(other.dataOriginTimespan)) {
      return false;
    }
    if (dataOwner == null) {
      if (other.dataOwner != null) {
        return false;
      }
    } else if (!dataOwner.equals(other.dataOwner)) {
      return false;
    }
    if (databaseUser == null) {
      if (other.databaseUser != null) {
        return false;
      }
    } else if (!databaseUser.equals(other.databaseUser)) {
      return false;
    }
    if (defaultTransactionIsolationLevel == null) {
      if (other.defaultTransactionIsolationLevel != null) {
        return false;
      }
    } else if (!defaultTransactionIsolationLevel.equals(other.defaultTransactionIsolationLevel)) {
      return false;
    }
    if (description == null) {
      if (other.description != null) {
        return false;
      }
    } else if (!description.equals(other.description)) {
      return false;
    }
    if (extraNameCharacters == null) {
      if (other.extraNameCharacters != null) {
        return false;
      }
    } else if (!extraNameCharacters.equals(other.extraNameCharacters)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (privileges == null) {
      if (other.privileges != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(privileges, other.privileges)) {
      return false;
    }
    if (producerApplication == null) {
      if (other.producerApplication != null) {
        return false;
      }
    } else if (!producerApplication.equals(other.producerApplication)) {
      return false;
    }
    if (productName == null) {
      if (other.productName != null) {
        return false;
      }
    } else if (!productName.equals(other.productName)) {
      return false;
    }
    if (productVersion == null) {
      if (other.productVersion != null) {
        return false;
      }
    } else if (!productVersion.equals(other.productVersion)) {
      return false;
    }
    if (roles == null) {
      if (other.roles != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(roles, other.roles)) {
      return false;
    }
    if (schemas == null) {
      if (other.schemas != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(schemas, other.schemas)) {
      return false;
    }
    if (stringFunctions == null) {
      if (other.stringFunctions != null) {
        return false;
      }
    } else if (!stringFunctions.equals(other.stringFunctions)) {
      return false;
    }
    if (supportsANSI92EntryLevelSQL == null) {
      if (other.supportsANSI92EntryLevelSQL != null) {
        return false;
      }
    } else if (!supportsANSI92EntryLevelSQL.equals(other.supportsANSI92EntryLevelSQL)) {
      return false;
    }
    if (supportsANSI92FullSQL == null) {
      if (other.supportsANSI92FullSQL != null) {
        return false;
      }
    } else if (!supportsANSI92FullSQL.equals(other.supportsANSI92FullSQL)) {
      return false;
    }
    if (supportsANSI92IntermediateSQL == null) {
      if (other.supportsANSI92IntermediateSQL != null) {
        return false;
      }
    } else if (!supportsANSI92IntermediateSQL.equals(other.supportsANSI92IntermediateSQL)) {
      return false;
    }
    if (supportsCoreSQLGrammar == null) {
      if (other.supportsCoreSQLGrammar != null) {
        return false;
      }
    } else if (!supportsCoreSQLGrammar.equals(other.supportsCoreSQLGrammar)) {
      return false;
    }
    if (systemFunctions == null) {
      if (other.systemFunctions != null) {
        return false;
      }
    } else if (!systemFunctions.equals(other.systemFunctions)) {
      return false;
    }
    if (timeDateFunctions == null) {
      if (other.timeDateFunctions != null) {
        return false;
      }
    } else if (!timeDateFunctions.equals(other.timeDateFunctions)) {
      return false;
    }
    if (url == null) {
      if (other.url != null) {
        return false;
      }
    } else if (!url.equals(other.url)) {
      return false;
    }
    if (users == null) {
      if (other.users != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(users, other.users)) {
      return false;
    }
    return true;
  }
}
