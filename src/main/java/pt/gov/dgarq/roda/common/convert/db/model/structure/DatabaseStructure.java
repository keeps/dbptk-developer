package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * 
 * @author Luis Faria
 * @author Miguel Coutada
 * 
 */
public class DatabaseStructure {

	private static final Logger logger = 
			Logger.getLogger(DatabaseStructure.class);

	private String name;
	
	private String description;
	
	private String archiver;
	
	private String archiverContact;
	
	private String dataOwner;
	
	private String dataOriginTimespan;
	
	private String producerApplication;	

	private String creationDate; 
	
	private String archivalDate;
	
	private String messageDigest;
	
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
		creationDate = null;
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
	 *            the database name
	 * @param creationDate 
	 *            date when the database was created, ISO 8601 format
	 * @param productName
	 *            the DBMS name
	 * @param productVersion
	 *            the DBMS version
	 * @param defaultTransactionIsolationLevel
	 *            database's default transaction isolation level
	 * @param extraNameCharacters
	 *            "extra" characters that can be used in unquoted identifier
	 *            names (those beyond a-z, A-Z, 0-9 and _)
	 * @param stringFunctions
	 *            comma-separated list of string functions available with this
	 *            database
	 * @param systemFunctions
	 *            comma-separated list of system functions available with this
	 *            database
	 * @param timeDateFunctions
	 *            comma-separated list of the time and date functions available
	 *            with this database
	 * @param url
	 *            URL for the DBMS
	 * @param supportsANSI92EntryLevelSQL
	 *            whether this database supports the ANSI92 entry level SQL
	 *            grammar
	 * @param supportsANSI92IntermediateSQL
	 *            whether this database supports the ANSI92 intermediate SQL
	 *            grammar
	 * @param supportsANSI92FullSQL
	 *            whether this database supports the ANSI92 full SQL grammar
	 * @param supportsCoreSQLGrammar
	 *            whether this database supports the ODBC Core SQL grammar
	 */
	
	// TODO complete doc
	public DatabaseStructure(String name, String description, String archiver,
			String archiverContact, String dataOwner,
			String dataOriginTimespan, String producerApplication,
			String creationDate, String messageDigest, String clientMachine,
			String productName, String productVersion, String databaseUser,
			Integer defaultTransactionIsolationLevel,
			String extraNameCharacters, String stringFunctions,
			String systemFunctions, String timeDateFunctions, String url,
			Boolean supportsANSI92EntryLevelSQL,
			Boolean supportsANSI92IntermediateSQL,
			Boolean supportsANSI92FullSQL, Boolean supportsCoreSQLGrammar,
			List<SchemaStructure> schemas, List<UserStructure> users,
			List<RoleStructure> roles, List<PrivilegeStructure> privileges) {
		super();
		this.name = name;
		this.description = description;
		this.archiver = archiver;
		this.archiverContact = archiverContact;
		this.dataOwner = dataOwner;
		this.dataOriginTimespan = dataOriginTimespan;
		this.producerApplication = producerApplication;
		this.creationDate = creationDate;
		this.messageDigest = messageDigest;
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
	 * @return the date when the database was created, ISO 8601 format
	 */
	public String getCreationDate() {
		return creationDate;
	}

	/**
	 * @param creationDate
	 *            the date when the database was created, ISO 8601 format
	 */
	public void setCreationDate(String creationDate) {
		this.creationDate = creationDate;
	}

	/**
	 * @return the date when the database was archived, ISO 8601 format
	 */
	public String getArchivalDate() {
		return archivalDate;
	}

	/**
	 * @param creationDate
	 *            the date when the database was archived, ISO 8601 format
	 */
	public void setArchivalDate(String archivalDate) {
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
	 *            database name
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
	 * 
	 * @param productName
	 *            the DBMS name
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
	 *            the DBMS version
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
	 *            database's default transaction isolation level
	 */
	public void setDefaultTransactionIsolationLevel(
			Integer defaultTransactionIsolationLevel) {
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
	 *            "extra" characters that can be used in unquoted identifier
	 *            names (those beyond a-z, A-Z, 0-9 and _)
	 */
	public void setExtraNameCharacters(String extraNameCharacters) {
		this.extraNameCharacters = extraNameCharacters;
	}

	/**
	 * @return comma-separated list of string functions available with this
	 *         database
	 */
	public String getStringFunctions() {
		return stringFunctions;
	}

	/**
	 * @param stringFunctions
	 *            comma-separated list of string functions available with this
	 *            database
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
	 *            whether this database supports the ANSI92 entry level SQL
	 *            grammar
	 */
	public void setSupportsANSI92EntryLevelSQL(
			Boolean supportsANSI92EntryLevelSQL) {
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
	 *            whether this database supports the ANSI92 full SQL grammar
	 */
	public void setSupportsANSI92FullSQL(Boolean supportsANSI92FullSQL) {
		this.supportsANSI92FullSQL = supportsANSI92FullSQL;
	}

	/**
	 * @return whether this database supports the ANSI92 intermediate SQL
	 *         grammar
	 */
	public Boolean getSupportsANSI92IntermediateSQL() {
		return supportsANSI92IntermediateSQL;
	}

	/**
	 * @param supportsANSI92IntermediateSQL
	 *            whether this database supports the ANSI92 intermediate SQL
	 *            grammar
	 */
	public void setSupportsANSI92IntermediateSQL(
			Boolean supportsANSI92IntermediateSQL) {
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
	 *            whether this database supports the ODBC Core SQL grammar
	 */
	public void setSupportsCoreSQLGrammar(Boolean supportsCoreSQLGrammar) {
		this.supportsCoreSQLGrammar = supportsCoreSQLGrammar;
	}

	/**
	 * @return comma-separated list of systemfunctions available with this
	 *         database
	 */
	public String getSystemFunctions() {
		return systemFunctions;
	}

	/**
	 * @param systemFunctions
	 *            comma-separated list of system functions available with this
	 *            database
	 */
	public void setSystemFunctions(String systemFunctions) {
		this.systemFunctions = systemFunctions;
	}

	/**
	 * @return comma-separated list of the time and date functions available
	 *         with this database
	 */
	public String getTimeDateFunctions() {
		return timeDateFunctions;
	}

	/**
	 * @param timeDateFunctions
	 *            comma-separated list of the time and date functions available
	 *            with this database
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
	 *            URL for the DBMS
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

	public String getMessageDigest() {
		return messageDigest;
	}

	public void setMessageDigest(String messageDigest) {
		this.messageDigest = messageDigest;
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
	 * @param tables
	 *            this database tables
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


	public void setPrivileges(List<PrivilegeStructure> privileges) {
		this.privileges = privileges;
	}
	
	/**
	 * Lookup a table structure by its table id
	 * 
	 * @param tableId
	 *            the table id
	 * @return the table structure
	 */
	public TableStructure lookupTableStructure(String tableId) {
		TableStructure ret = null;
		for (SchemaStructure schema : getSchemas()) {
			for (TableStructure tableStructure : schema.getTables()) {
				if (tableStructure.getId().equals(tableId)) {
					ret = tableStructure;
				}
			}
		}
		return ret;
	}

	/**
	 * Sort the tables topologically by its foreign key references. This method
	 * is useful when inserting data into the database, so the foreign key
	 * constrains will be respected
	 * 
	 * @param tables
	 * @return the sorted table list or null if the tables cannot be sorted
	 *         topologically (recursive graph)
	 */
	public static List<TableStructure> topologicSort(
			List<TableStructure> tables) {
		
		List<TableStructure> sortedTables = new ArrayList<TableStructure>(
				tables.size());
		boolean canSortTopologically = true;
		while (sortedTables.size() != tables.size() && canSortTopologically) {
			List<TableStructure> filtered = filterReferencedTables(tables,
					sortedTables);
			if (filtered.size() > 0) {
				sortedTables.addAll(filtered);
			} else {
				canSortTopologically = false;
				sortedTables = null;
				logger.error("Cannot sort topologicaly");
			}
		}
		return sortedTables;
	}

	private static List<TableStructure> filterReferencedTables(
			List<TableStructure> allTables, 
			List<TableStructure> insertedTables) {
		
		List<TableStructure> referencedTables = new Vector<TableStructure>();
		for (TableStructure table : allTables) {
			if (!insertedTables.contains(table)) {
				boolean allReferredTablesInserted = true;
				for (ForeignKey fkey : table.getForeignKeys()) {
					if (!containsTable(
							insertedTables, fkey.getReferencedTable())) {
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

	private static boolean containsTable(List<TableStructure> tables,
			String tableId) {
		boolean foundIt = false;
		for (TableStructure table : tables) {
			if (table.getId().equals(tableId)) {
				foundIt = true;
				break;
			}
		}
		return foundIt;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DatabaseStructure [name=");
		builder.append(name);
		builder.append("\n");
		builder.append("creationDate=");
		builder.append(creationDate);
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
}
