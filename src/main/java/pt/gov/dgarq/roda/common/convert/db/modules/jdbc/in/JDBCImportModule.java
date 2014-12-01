package pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.w3c.util.DateParser;

import pt.gov.dgarq.roda.common.FileFormat;
import pt.gov.dgarq.roda.common.FormatUtility;
import pt.gov.dgarq.roda.common.convert.db.Main;
import pt.gov.dgarq.roda.common.convert.db.model.data.BinaryCell;
import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.data.FileItem;
import pt.gov.dgarq.roda.common.convert.db.model.data.Row;
import pt.gov.dgarq.roda.common.convert.db.model.data.SimpleCell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.InvalidDataException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.CandidateKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.CheckConstraint;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ForeignKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.PrimaryKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.PrivilegeStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Reference;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoleStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoutineStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.Trigger;
import pt.gov.dgarq.roda.common.convert.db.model.structure.UserStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ViewStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeArray;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.UnsupportedDataType;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseImportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class JDBCImportModule implements DatabaseImportModule {

	// if fetch size is zero, then the driver decides the best fetch size
	protected static final int ROW_FETCH_BLOCK_SIZE = 0;

	protected static final String DEFAULT_DATA_TIMESPAN = "(...)";
	
	protected static final boolean IGNORE_UNSUPPORTED_DATA_TYPES = true;
	
	private final Logger logger = Logger.getLogger(JDBCImportModule.class);

	protected final String driverClassName;

	protected final String connectionURL;

	protected Connection connection;

	protected Statement statement;

	protected DatabaseMetaData dbMetadata;

	protected DatabaseStructure dbStructure;

	protected SQLHelper sqlHelper;

	protected FormatUtility formatUtility;

	/**
	 * Create a new JDBC import module
	 * 
	 * @param driverClassName
	 *            the name of the the JDBC driver class
	 * @param connectionURL
	 *            the connection url to use in the connection
	 */
	public JDBCImportModule(String driverClassName, String connectionURL) {
		this(driverClassName, connectionURL, new SQLHelper());
	}

	protected JDBCImportModule(String driverClassName, String connectionURL,
			SQLHelper sqlHelper) {
		this.driverClassName = driverClassName;
		this.connectionURL = connectionURL;
		this.sqlHelper = sqlHelper;
		connection = null;
		dbMetadata = null;
		dbStructure = null;
	}

	/**
	 * Connect to the server using the properties defined in the constructor, or
	 * return the existing connection
	 * 
	 * @return the connection
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 *             the JDBC driver could not be found in classpath
	 */
	public Connection getConnection() throws SQLException,
			ClassNotFoundException {
		if (connection == null) {
			logger.debug("Loading JDBC Driver " + driverClassName);
			Class.forName(driverClassName);
			logger.debug("Getting connection");
			connection = DriverManager.getConnection(connectionURL);
			logger.debug("Connected");
		}
		return connection;
	}

	protected Statement getStatement() throws SQLException,
			ClassNotFoundException {
		if (statement == null) {
			statement = getConnection().createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
					ResultSet.CLOSE_CURSORS_AT_COMMIT);
		}
		return statement;
	}

	/**
	 * Get the database metadata
	 * 
	 * @return the database metadata
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public DatabaseMetaData getMetadata() throws SQLException,
			ClassNotFoundException {
		if (dbMetadata == null) {
			dbMetadata = getConnection().getMetaData();
		}
		return dbMetadata;
	}

	/**
	 * Close current connection
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		if (connection != null) {
			connection.close();
			connection = null;
			dbMetadata = null;
			dbStructure = null;
		}
	}

	/**
	 * @return the database structure
	 * @throws SQLException
	 * @throws UnknownTypeException
	 *             the original data type is unknown
	 * @throws ClassNotFoundException
	 */
	protected DatabaseStructure getDatabaseStructure() 
			throws SQLException, UnknownTypeException, ClassNotFoundException {
		if (dbStructure == null) {
			logger.debug("Importing structure");
			dbStructure = new DatabaseStructure();
			logger.debug("driver version: " + getMetadata().getDriverVersion());
			dbStructure.setName(getDbName());
			dbStructure.setProductName(getMetadata().getDatabaseProductName());
			dbStructure.setProductVersion(getMetadata()
					.getDatabaseProductVersion());
			dbStructure.setDataOwner(System.getProperty("user.name"));
			dbStructure.setDataOriginTimespan(DEFAULT_DATA_TIMESPAN);
			dbStructure.setProducerApplication(
					Main.APP_NAME);
			String clientMachine = "";
			try {
				clientMachine = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {}
			dbStructure.setClientMachine(clientMachine);
		
			dbStructure.setSchemas(getSchemas());			
			dbStructure.setUsers(getUsers());			
			dbStructure.setRoles(getRoles());
			dbStructure.setPrivileges(getPrivileges());
				
			logger.debug("Finishing get dbStructure");
		}	
		return dbStructure;
	}
	
	protected String getDbName() throws SQLException, ClassNotFoundException {
		return getConnection().getCatalog();
	}
	
	/**
	 * Checks if schema name matches the set of schemas to be ignored.
	 * @param schemaName
	 * 			  the schema name
	 * @return true if schema is ignored; false if it isn't
	 */
	protected boolean isIgnoredImportedSchema(String schemaName) {
		boolean ignoredSchema = false;
		for (String s : getIgnoredImportedSchemas()) {
			if (schemaName.matches(s)) {
				ignoredSchema = true;
			}
		}
		return ignoredSchema;
	}
	
	/**
	 * 
	 * @return the database schemas (not ignored by default and/or user)
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws UnknownTypeException
	 */
	protected List<SchemaStructure> getSchemas() 
			throws SQLException, ClassNotFoundException, UnknownTypeException {		
		List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
		
		ResultSet rs = getMetadata().getSchemas();	
		while (rs.next()) {
			String schemaName = rs.getString(1);
			// does not import ignored schemas
			if (isIgnoredImportedSchema(schemaName)) {
				continue;
			}
			schemas.add(getSchemaStructure(schemaName));
		}
		return schemas;
	}

	/**
	 * Get schemas that won't be imported
	 * 
	 * Accepts schemas names in as regular expressions
	 * I.e. SYS.* will ignore SYSCAT, SYSFUN, etc
	 *  
	 * @return
	 * 			  the schema names not to be imported
	 */
	protected Set<String> getIgnoredImportedSchemas() {
		return new HashSet<String>();
	}

	/**
	 * 
	 * @param schemaName
	 * 			  the schema name
	 * @return the schema structure of a given schema name
	 * @throws ModuleException 
	 */
	protected SchemaStructure getSchemaStructure(String schemaName) 
			throws SQLException, UnknownTypeException, ClassNotFoundException {
		SchemaStructure schema = new SchemaStructure();
		schema.setName(schemaName);
		schema.setFolder(schemaName);

		schema.setTables(getTables(schema));
		schema.setViews(getViews(schemaName));
		schema.setRoutines(getRoutines(schemaName));
		
		return schema;
	}
	
	/**
	 * 
	 * @param schema
	 * 			  the schema structure
	 * @return the database tables of a given schema
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws UnknownTypeException
	 */
	protected List<TableStructure> getTables(SchemaStructure schema) 
			throws SQLException, ClassNotFoundException, UnknownTypeException {
		List<TableStructure> tables = new ArrayList<TableStructure>();		
		ResultSet rset = getMetadata().getTables(dbStructure.getName(),
				schema.getName(), "%", new String[] { "TABLE" });
		while (rset.next()) {
			logger.debug("getting table structure for: " + rset.getString(3));
			tables.add(getTableStructure(schema, rset.getString(3)));
		}
		return tables;
	}
	
	/**
	 * 
	 * @param schemaName
	 * 			  the schema name
	 * @return the database views of a given schema
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws UnknownTypeException
	 */
	protected List<ViewStructure> getViews(String schemaName) 
			throws SQLException, ClassNotFoundException, UnknownTypeException {
		List<ViewStructure> views = new ArrayList<ViewStructure>();
		ResultSet rset = getMetadata().getTables(dbStructure.getName(), 
				schemaName, "%", new String[] { "VIEW" });
		while (rset.next()) {
			String viewName = rset.getString(3);
			ViewStructure view = new ViewStructure();
			view.setName(viewName);
			view.setColumns(getColumns(schemaName, viewName));
			views.add(view);
		}
		return views;
	}
	
	/**
	 * 
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	protected List<RoutineStructure> getRoutines(String schemaName) 
			throws SQLException, ClassNotFoundException {
		// TODO add optional fields to routine (use getProcedureColumns)
		List<RoutineStructure> routines = new ArrayList<RoutineStructure>();
		
		ResultSet rset = getMetadata().getProcedures(
				dbStructure.getName(), schemaName, "%");
		while (rset.next()) {
			String routineName = rset.getString(3);			
			RoutineStructure routine = new RoutineStructure();
			routine.setName(routineName);			
			if (rset.getString(7) != null) {
				routine.setDescription(rset.getString(7));
			} else {
				if (rset.getShort(8) == 1) {				
					routine.setDescription("Procedure does not "
							+ "return a result");
				} else if (rset.getShort(8) == 2) {
					routine.setDescription("Procedure returns a result");
				}
			}
			routines.add(routine);
		}		
		return routines;
	}
	
	/**
	 * @param tableName
	 *            the name of the table
	 * @return the table structure
	 * @throws SQLException
	 * @throws UnknownTypeException
	 *             the original data type is unknown
	 * @throws ClassNotFoundException
	 * @throws ModuleException 
	 */
	protected TableStructure getTableStructure(
			SchemaStructure schema, String tableName) 
			throws SQLException, UnknownTypeException, ClassNotFoundException {

		TableStructure table = new TableStructure();
		table.setId(schema.getName() + "." + tableName);
		table.setName(tableName);         
		table.setFolder(tableName);
		table.setSchema(schema);

		table.setColumns(getColumns(schema.getName(), tableName));		
		table.setPrimaryKey(getPrimaryKey(schema.getName(), tableName));
		table.setForeignKeys(getForeignKeys(schema.getName(), tableName));
		table.setCandidateKeys(getCandidateKeys(schema.getName(), tableName));
		table.setCheckConstraints(
				getCheckConstraints(schema.getName(), tableName));
		table.setTriggers(getTriggers(schema.getName(), tableName));
		
		return table;
	}
		
	/**
	 * Create the column structure
	 * 
	 * @param tableName
	 *            the name of the table which the column belongs to
	 * @param columnName
	 *            the name of the column
	 * @param type
	 *            the type of the column
	 * @param nillable
	 *            is the column nillable
	 * @param index
	 *            the column index
	 * @param description
	 *            the column description
	 * @return the column structure
	 */
	protected ColumnStructure getColumnStructure(String tableName,
			String columnName, Type type, Boolean nillable, int index,
			String description) {
		ColumnStructure column = new ColumnStructure(tableName + "."
				+ columnName, columnName, type, nillable, description);	
		return column;
	}
	
	protected List<UserStructure> getUsers() 
			throws SQLException, ClassNotFoundException {
		List<UserStructure> users = new ArrayList<UserStructure>();
		String query = sqlHelper.getUsersSQL(getDbName());
		if (query != null) {
			ResultSet rs = getStatement().executeQuery(query);
			while (rs.next()) {
				UserStructure user = new UserStructure();
				user.setName(rs.getString("USER_NAME"));
				users.add(user);
			}
		} else {
			users.add(new UserStructure("UNDEFINED_USER", "DESCRIPTION"));
			logger.warn("Users were not imported: not supported yet on " 
					+ getClass().getSimpleName() + "\n"
					+ "UNDEFINED_USER will be set as user name");
		}
		return users;
	}

	/**
	 * 
	 * @return the database roles 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	protected List<RoleStructure> getRoles() 
			throws SQLException, ClassNotFoundException {
		List<RoleStructure> roles = new ArrayList<RoleStructure>();
		String query = sqlHelper.getRolesSQL();
		if (query != null) {
			ResultSet rs = getStatement().executeQuery(query);
			while(rs.next()) {
				RoleStructure role = new RoleStructure();
				String roleName;
				try {
					roleName = rs.getString("ROLE_NAME");
				} catch (SQLException e) {
					roleName = "";
				}
				role.setName(roleName);
				
				String admin;
				try {
					admin = rs.getString("ADMIN");
				} catch (SQLException e) {
					admin = "";
				}
				role.setAdmin(admin);
				
				roles.add(role);
			}
		}
		else {
			logger.info("Roles were not imported: not supported yet on " 
					+ getClass().getSimpleName());
		}
		return roles;
	}
	
	/**
	 * 
	 * @return the database privileges
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	protected List<PrivilegeStructure> getPrivileges() 
			throws SQLException, ClassNotFoundException {
		List<PrivilegeStructure> privileges = 
				new ArrayList<PrivilegeStructure>();
		
		for (SchemaStructure schema : dbStructure.getSchemas()) {
			for (TableStructure table : schema.getTables()) {
				ResultSet rs = getMetadata().getTablePrivileges(
						dbStructure.getName(), schema.getName(), 
						table.getName());
				while (rs.next()) {
					PrivilegeStructure privilege = new PrivilegeStructure();
					String grantor = rs.getString("GRANTOR");
					if (grantor == null) {
						grantor = "";
					}
					privilege.setGrantor(grantor);
					
					String grantee = rs.getString("GRANTEE");
					if (grantee == null) {
						grantee = "";
					}
					privilege.setGrantee(grantee);					
					privilege.setType(rs.getString("PRIVILEGE"));
					
					String option = "false";
					String isGrantable = rs.getString("IS_GRANTABLE");
					if (isGrantable != null) {
						if (isGrantable.equalsIgnoreCase("yes")) {
							option = "true";
						}
					}
					privilege.setOption(option);
					privilege.setObject("TABLE \"" + schema.getName() 
							+ "\".\"" + table.getName() + "\"");
					
					privileges.add(privilege);
				}
			}
		}
		return privileges;
	}
	
	/**
	 * 
	 * @param schemaName
	 * 			  the schema name
	 * @param tableName
	 * 			  the table name
	 * @return the columns of a given schema.table
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws UnknownTypeException
	 */
	protected List<ColumnStructure> getColumns(
			String schemaName, String tableName) 
			throws SQLException, ClassNotFoundException, UnknownTypeException {
		
		// logger.debug("id: " + schemaName + "." + tableName);
		List<ColumnStructure> columns = new ArrayList<ColumnStructure>();
		ResultSet rs = getMetadata().getColumns(dbStructure.getName(), 
				schemaName, tableName, "%");

		while (rs.next()) {
			String columnName = rs.getString(4);
			String isNullable = rs.getString(18);
			int dataType = rs.getInt(5);
			String typeName = rs.getString(6);
			int columnSize = rs.getInt(7);
			int decimalDigits = rs.getInt(9);
			int numPrecRadix = rs.getInt(10);
			int index = rs.getInt(17);
			String remarks = rs.getString(12);

			Boolean nillable = Boolean.TRUE;
			if (isNullable != null && isNullable.equals("NO")) {
				nillable = Boolean.FALSE;
			}
			
//			logger.debug("(" + tableName + ") " + "colName: " + columnName 
//					+ "; dataType: " + dataType + "; typeName: " + typeName);
			Type columnType = getType(dataType, typeName, columnSize,
					decimalDigits, numPrecRadix);

			ColumnStructure column = getColumnStructure(tableName, columnName,
					columnType, nillable, index, remarks);	

			columns.add(column);
		}
		return columns;
	}

	/**
	 * Map the original type to the normalized type model 
	 * saving the appropriate SQL:99 data type info 
	 * 
	 * @param originalDataType
	 *            the name of the original data type
	 * @param originalMaxSize
	 *            the max size of the original data type
	 * @return the normalized type
	 * @throws UnknownTypeException
	 *             the original type is unknown and cannot be mapped
	 */
	protected Type getType(int dataType, String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) throws UnknownTypeException {
		Type type;
		switch (dataType) {
		case Types.BIGINT:
			type = new SimpleTypeNumericExact(Integer.valueOf(columnSize),
					Integer.valueOf(decimalDigits));
			type.setSql99TypeName("NUMERIC");
			break;
		case Types.BINARY:
			type = getBinaryType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.BIT:
			if (columnSize > 1) {
				type = new SimpleTypeBinary(Integer.valueOf(columnSize));
				type.setSql99TypeName("BIT");
			} else {
				type = new SimpleTypeBoolean();
				type.setSql99TypeName("BOOLEAN");
			}
			break;
		case Types.BLOB:
			type = new SimpleTypeBinary(Integer.valueOf(columnSize));
			type.setSql99TypeName("BINARY LARGE OBJECT");
			break;
		case Types.BOOLEAN:
			type = new SimpleTypeBoolean();
			type.setSql99TypeName("BOOLEAN");
			break;
		case Types.CHAR:
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.FALSE);
			type.setSql99TypeName("CHARACTER");
			break;
		case Types.NCHAR:
			// TODO add charset
			type = new SimpleTypeString(Integer.valueOf(columnSize), 
					Boolean.FALSE);
			type.setSql99TypeName("CHARACTER");
			break;
		case Types.CLOB:
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.TRUE);
			type.setSql99TypeName("CHARACTER LARGE OBJECT");
			break;
		case Types.DATE:
			type = new SimpleTypeDateTime(Boolean.FALSE, Boolean.FALSE);
			type.setSql99TypeName("DATE");
			break;
		case Types.DECIMAL:
			type = getDecimalType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.DOUBLE:
			type = getDoubleType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.FLOAT:
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("FLOAT");
			break;
		case Types.INTEGER:
			type = new SimpleTypeNumericExact(Integer.valueOf(columnSize),
					Integer.valueOf(decimalDigits));
			type.setSql99TypeName("INTEGER");
			break;
		case Types.LONGVARBINARY:
			type = getLongvarbinaryType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.LONGVARCHAR:
			type = getLongvarcharType(typeName, columnSize, decimalDigits,
					numPrecRadix);
			break;
		case Types.LONGNVARCHAR:
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.TRUE);
			type.setSql99TypeName("CHARACTER LARGE OBJECT");
			break;
		case Types.NUMERIC:
			type = getNumericType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.REAL:
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("REAL");
			break;
		case Types.SMALLINT:
			type = new SimpleTypeNumericExact(Integer.valueOf(columnSize),
					Integer.valueOf(decimalDigits));
			type.setSql99TypeName("SMALLINT");
			break;
		case Types.TIME:
			type = getTimeType(
					typeName, columnSize, decimalDigits, numPrecRadix);
			break;
		case Types.TIMESTAMP:
			type = getTimestampType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.TINYINT:
			type = new SimpleTypeNumericExact(Integer.valueOf(columnSize),
					Integer.valueOf(decimalDigits));
			type.setSql99TypeName("SMALLINT");
			break;
		case Types.VARBINARY:
			type = new SimpleTypeBinary(Integer.valueOf(columnSize));
			type.setSql99TypeName("BIT VARYING");
			break;
		case Types.VARCHAR:
			type = getVarcharType(typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
		case Types.NVARCHAR:
			// TODO add charset
			type = new SimpleTypeString(Integer.valueOf(columnSize), 
					Boolean.TRUE);
			type.setSql99TypeName("CHARACTER VARYING");
			break;
		case Types.ARRAY:
			// TODO add array type convert support
			throw new UnknownTypeException("Array type not yet supported");

		case Types.STRUCT:
			// TODO add struct type convert support
			throw new UnknownTypeException("Struct type not yet supported");
		
		case Types.OTHER:
			logger.debug("OTHER");
			type = getOtherType(dataType, typeName, columnSize, decimalDigits, 
					numPrecRadix);
			break;
			
		default:
			// tries to get specific DBMS data types
			type = getSpecificType(dataType, typeName, columnSize, 
					decimalDigits, numPrecRadix);
			break;
		}
		type.setOriginalTypeName(typeName);
		return type;
	}
	
	protected Type getBinaryType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeBinary(Integer.valueOf(columnSize));
		type.setSql99TypeName("BIT");
		return type;
	}

	protected Type getDecimalType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeNumericExact(Integer.valueOf(columnSize),
				Integer.valueOf(decimalDigits));
		type.setSql99TypeName("DECIMAL");
		return type;
	}
	
	protected Type getNumericType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeNumericExact(
				Integer.valueOf(columnSize), Integer.valueOf(decimalDigits));
		type.setSql99TypeName("NUMERIC");
		return type;
	}
	
	protected Type getDoubleType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		Type type = 
				new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
		type.setSql99TypeName("DOUBLE PRECISION");
		return type;
	}
	
	protected Type getLongvarbinaryType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeBinary(Integer.valueOf(columnSize));
		type.setSql99TypeName("BINARY LARGE OBJECT");
		return type;
	}
	
	protected Type getLongvarcharType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) throws UnknownTypeException {
		Type type = new SimpleTypeString(Integer.valueOf(columnSize),
				Boolean.TRUE);
		type.setSql99TypeName("CHARACTER LARGE OBJECT");
		return type;
	}
	
	protected Type getTimeType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		type.setSql99TypeName("TIME");
		return type;
	}
	
	protected Type getTimestampType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		type.setSql99TypeName("TIMESTAMP");
		return type;
	}
		
	protected Type getVarcharType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) {
		Type type = new SimpleTypeString(Integer.valueOf(columnSize),
				Boolean.TRUE);
		type.setSql99TypeName("CHARACTER VARYING");
		return type;
	}
	
	/**
	 * Gets data types defined as Types.OTHER. The data type is inferred 
	 * by typeName, sometimes specific to each DBMS 
	 * 
	 * @param dataType
	 * @param typeName
	 * @param columnSize
	 * @param numPrecRadix 
	 * @param decimalDigits
	 *  
	 * @return the inferred data type
	 * @throws UnknownTypeException
	 */
	protected Type getOtherType(int dataType, String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix)
					throws UnknownTypeException {
		return getUnsupportedDataType(dataType, typeName, columnSize, 
				decimalDigits, numPrecRadix);
	}
	
	/**
	 * Gets specific DBMS data types. 
	 * E.g.:OracleTypes.BINARY_DOUBLE
	 * 
	 * @param dataType
	 * @param typeName
	 * @param columnSize
	 * @param numPrecRadix 
	 * @param decimalDigits
	 *  
	 * @return the inferred data type
	 * @throws UnknownTypeException
	 */
	protected Type getSpecificType(int dataType, String typeName, 
			int columnSize, int decimalDigits, int numPrecRadix) 
					throws UnknownTypeException {
		return getUnsupportedDataType(dataType, typeName, columnSize, 
				decimalDigits, numPrecRadix);
	}
	
	/**
	 * Gets the UnsupportedDataType. 
	 * This data type is a placeholder for unsupported data types
	 * 
	 * @param dataType
	 * @param typeName
	 * @param columnSize
	 * @param decimalDigits
	 * @param numPrecRadix
	 * @return
	 * @throws UnknownTypeException
	 */
	protected Type getUnsupportedDataType(int dataType, String typeName, 
			int columnSize, int decimalDigits, int numPrecRadix) 
					throws UnknownTypeException {
		if (IGNORE_UNSUPPORTED_DATA_TYPES) {
			return new UnsupportedDataType(dataType, typeName, columnSize, 
					decimalDigits, numPrecRadix);
		} else {
			throw new UnknownTypeException("Unsupported JDBC type, code: "
					+ dataType);
		}		
	}

	/**
	 * Get the table primary key
	 * 
	 * @param tableName
	 *            the name of the table
	 * @return the primary key
	 * @throws SQLException
	 * @throws UnknownTypeException
	 * @throws ClassNotFoundException
	 * @throws ModuleException 
	 */
	protected PrimaryKey getPrimaryKey(String schemaName, String tableName) 
			throws SQLException, UnknownTypeException, ClassNotFoundException {
		String pkName = null;
		List<String> pkColumns = new Vector<String>();

		ResultSet rs = getMetadata().getPrimaryKeys(
				getDatabaseStructure().getName(), schemaName, tableName);
		
		while (rs.next()) {
			pkName = rs.getString(6);
			pkColumns.add(rs.getString(4));
		}
		
		if (pkName == null) {
			pkName = tableName + "_pkey";
		}

		PrimaryKey pk = new PrimaryKey();
		pk.setName(pkName);
		pk.setColumnNames(pkColumns);
		return !pkColumns.isEmpty() ? pk : null;
	}

	/**
	 * Get the table foreign keys
	 * 
	 * @param schemaName
	 * 			  the name of the schema
	 * @param tableName
	 *            the name of the table
	 * @return the foreign keys
	 * @throws SQLException
	 * @throws UnknownTypeException
	 * @throws ClassNotFoundException
	 * @throws ModuleException 
	 */
	protected List<ForeignKey> getForeignKeys(
			String schemaName, String tableName) 
			throws SQLException, UnknownTypeException, ClassNotFoundException {
		
		List<ForeignKey> foreignKeys = new Vector<ForeignKey>();
		
		ResultSet rs = getMetadata().getImportedKeys(
				getDatabaseStructure().getName(), schemaName, tableName);
		while (rs.next()) {
			List<Reference> references = new ArrayList<Reference>();
			boolean found = false;
			Reference reference = new Reference(rs.getString("FKCOLUMN_NAME"), 
					rs.getString("PKCOLUMN_NAME"));
			
			String fkeyName = rs.getString("FK_NAME");
			if (fkeyName == null) {
				fkeyName = "FK_" + rs.getString("FKCOLUMN_NAME");
			}
			
			for (ForeignKey key : foreignKeys) {
				if (key.getName().equals(fkeyName)) {
					references = key.getReferences();
					references.add(reference);
					key.setReferences(references);
					found = true;
					break;
				}
			}
			
			if (!found) {
				ForeignKey fkey = new ForeignKey();
				fkey.setId(tableName + "." + rs.getString("FKCOLUMN_NAME"));
				fkey.setName(fkeyName);
				fkey.setReferencedSchema(
						getReferencedSchema(rs.getString("PKTABLE_SCHEM")));
				fkey.setReferencedTable(rs.getString("PKTABLE_NAME"));
				references.add(reference);
				fkey.setReferences(references);
				// TODO add: fkey.setMatchType(??);
				fkey.setUpdateAction(getUpdateRule(rs.getShort("UPDATE_RULE")));
				fkey.setDeleteAction(getDeleteRule(rs.getShort("DELETE_RULE")));
				foreignKeys.add(fkey);
			}
		}
		return foreignKeys;
	}
	
	protected String getReferencedSchema(String s) 
			throws SQLException, ClassNotFoundException {	
		return s;
	}
	
	/**
	 * Gets the name of the update rule
	 * 
	 * @param value
	 * @return
	 */
	protected String getUpdateRule(Short value) {
		String rule = null;
		switch(value) {
			case 0: rule = "CASCADE"; break;
			case 1: rule = "RESTRICT"; break;
			case 2: rule = "SET NULL"; break;
			case 3: rule = "NO ACTION"; break;
			case 4: rule = "SET DEFAULT"; break;
			default: rule = "SET DEFAULT"; break;
		}
		return rule;
	}
	
	/**
	 * Gets the name of the delete rule
	 * 
	 * @param value
	 * @return
	 */
	protected String getDeleteRule(Short value) {
		return getUpdateRule(value);
	}
	

	/**
	 * Gets the candidate keys of a given schema table
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	// VERIFY adding PKs
	protected List<CandidateKey> getCandidateKeys(String schemaName, 
			String tableName) throws SQLException, ClassNotFoundException {
		List<CandidateKey> candidateKeys = new ArrayList<CandidateKey>();
		
		ResultSet rs = getMetadata().getIndexInfo(dbStructure.getName(),
				schemaName, tableName, true, true);
		while (rs.next()) {
			List<String> columns = new ArrayList<String>();
			boolean found = false;
			
			for (CandidateKey key : candidateKeys) {
				if (key.getName().equals(rs.getString(6))) {
					columns = key.getColumns(); 
					columns.add(rs.getString(9));
					key.setColumns(columns);
					found = true;
					break;
				}
			}
			
			if (!found) {
				if (rs.getString(6) != null) {
					CandidateKey candidateKey = new CandidateKey();
					candidateKey.setName(rs.getString(6));
					columns.add(rs.getString(9));
					candidateKey.setColumns(columns);
					candidateKeys.add(candidateKey);
				}
			}
		}
		return candidateKeys;		
	}

	/**
	 * Gets the check constraints of a given schema table
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected List<CheckConstraint> getCheckConstraints(String schemaName, 
			String tableName) throws ClassNotFoundException {
		List<CheckConstraint> checkConstraints = 
				new ArrayList<CheckConstraint>();
		
		String query = sqlHelper.getCheckConstraintsSQL(schemaName, tableName);
		if (query != null) {
			try {	
				ResultSet rs = getStatement().executeQuery(query);
				while (rs.next()) {
					CheckConstraint checkConst = new CheckConstraint();
					
					String checkName;
					try {
						checkName = rs.getString("CHECK_NAME");
					} catch (SQLException e) {
						checkName = "";
					}
					checkConst.setName(checkName);
					
					String checkCondition;
					try {
						checkCondition = rs.getString("CHECK_CONDITION");
					} catch (SQLException e) {
						checkCondition = "UNKNOWN";
					}
					checkConst.setCondition(checkCondition);
					
					String checkDescription;
					try {
						checkDescription = rs.getString("CHECK_DESCRIPTION");
					} catch (SQLException e) {
						checkDescription = null;
					}
					if (checkDescription != null) {
						checkConst.setDescription(checkDescription);
					}
					checkConstraints.add(checkConst);
				}
			} catch (SQLException e) {
				String message = "Check constraints were not imported for " 
						+ schemaName + "." + tableName + ". ";
				if (query.equals("")) {
					message += "Not supported by " + sqlHelper.getName();
				} else {
					message += "An error occurred!";
				}
				logger.info(message);
			}
		} else {
			logger.info(
					"Check constraints were not imported: not supported yet on "
							+ getClass().getSimpleName());
		}
		return checkConstraints;
	}
	
	/**
	 * Gets the triggers of a given schema table
	 * 
	 * @param schemaName
	 * @param tableName
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected List<Trigger> getTriggers(String schemaName, String tableName) 
			throws ClassNotFoundException {
		List<Trigger> triggers = new ArrayList<Trigger>();
		
		String query = sqlHelper.getTriggersSQL(schemaName, tableName);
		if (query != null) {
			try { 
				ResultSet rs = getStatement().executeQuery(
						sqlHelper.getTriggersSQL(schemaName, tableName));
			
				while (rs.next()) {
					Trigger trigger = new Trigger();
					
					String triggerName;
					try {
						triggerName = rs.getString("TRIGGER_NAME");
					} catch (SQLException e) {
						triggerName = "";
					}
					trigger.setName(triggerName);
					
					String actionTime; 
					try {
						actionTime = 
								processActionTime(rs.getString("ACTION_TIME"));
					} catch (SQLException e) {
						actionTime = "";
					}
					trigger.setActionTime(actionTime);
					
					String triggerEvent;
					try {
						triggerEvent = processTriggerEvent(
								rs.getString("TRIGGER_EVENT"));
					} catch (SQLException e) {
						triggerEvent = "";
					}
					trigger.setTriggerEvent(triggerEvent);
					
					String triggeredAction;
					try {
						triggeredAction = rs.getString("TRIGGERED_ACTION");
					} catch (SQLException e) {
						triggeredAction = "";
					}
					trigger.setTriggeredAction(triggeredAction);
					
					String description;
					try {
						description = rs.getString("REMARKS");
					} catch (SQLException e) {
						description = null;
					}
					if (description != null) {
						trigger.setDescription(description);		
					}
					
					triggers.add(trigger);
				}
			} catch (SQLException e) {
				logger.info("No triggers imported for " + schemaName 
						+ "." + tableName);
			}
		} else {
			logger.info("Triggers were not imported: not supported yet on "
					+ getClass().getSimpleName());
		}
		return triggers;
	}

	/**
	 * Sanitizes the trigger event data  
	 * 
	 * @param string
	 * @return
	 */
	protected String processTriggerEvent(String string) {
		return string;
	}

	/**
	 * Sanitizes the trigger action time data
	 * 
	 * @param string
	 * @return
	 */
	protected String processActionTime(String string) {
		return string;
	}

	protected Row convertRawToRow(ResultSet rawData,
			TableStructure tableStructure) throws InvalidDataException,
			SQLException, ClassNotFoundException, ModuleException {
		Row row = null;
		if (isRowValid(rawData, tableStructure)) {
			List<Cell> cells = new ArrayList<Cell>(tableStructure.getColumns()
					.size());
			for (int i = 0; i < tableStructure.getColumns().size(); i++) {
				ColumnStructure colStruct = tableStructure.getColumns().get(i);
				cells.add(convertRawToCell(tableStructure.getName(), colStruct
						.getName(), i + 1, rawData.getRow(), colStruct
						.getType(), rawData));
			}
			row = new Row(rawData.getRow(), cells);
		}
		return row;
	}

	protected Cell convertRawToCell(String tableName, String columnName,
			int columnIndex, int rowIndex, Type cellType, ResultSet rawData)
			throws SQLException, InvalidDataException, ClassNotFoundException,
			ModuleException {
		Cell cell;
		String id = tableName + "." + columnName + "." + rowIndex;
		if (cellType instanceof ComposedTypeArray) {
			// Array array = rawData.getArray(index);
			// TODO convert array to cell
			throw new InvalidDataException(
					"Convert data of array type not yet supported");
		} else if (cellType instanceof ComposedTypeStructure) {
			// TODO get structure and convert to cell
			throw new InvalidDataException(
					"Convert data of struct type not yet supported");
		} else if (cellType instanceof SimpleTypeBoolean) {
			cell = new SimpleCell(id, rawData.getBoolean(columnName) ? "true"
					: "false");
		} else if (cellType instanceof SimpleTypeNumericApproximate) {
			cell = rawToCellSimpleTypeNumericApproximate(id, columnName, 
					cellType, rawData);
		} else if (cellType instanceof SimpleTypeDateTime) {
			cell = rawToCellSimpleTypeDateTime(
					id, columnName, cellType, rawData);
		} else if (cellType instanceof SimpleTypeBinary) {
			cell = rawToCellSimpleTypeBinary(id, columnName, cellType, rawData);
		} else if (cellType instanceof UnsupportedDataType) {
			cell = new SimpleCell(id, "UNSUPPORTED DATA TYPE");
		} else {
			cell = new SimpleCell(id, rawData.getString(columnName));
		}
		return cell;
	}
	
	protected Cell rawToCellSimpleTypeNumericApproximate(String id, 
			String columnName, Type cellType, ResultSet rawData) 
					throws SQLException {
		return new SimpleCell(id, rawData.getString(columnName));		
	}
		
	protected Cell rawToCellSimpleTypeDateTime(String id, String columnName, 
			Type cellType, ResultSet rawData) throws SQLException {
		Cell cell = null;
		SimpleTypeDateTime undefinedDate = (SimpleTypeDateTime) cellType;
		if (undefinedDate.getTimeDefined()) {
			if (cellType.getSql99TypeName().equalsIgnoreCase("TIME")) {
				Time time = rawData.getTime(columnName);
				if (time != null) {
					cell = new SimpleCell(id, time.toString());
				} else {
					cell = new SimpleCell(id, null);
				}
			} else {
				Timestamp timestamp = rawData.getTimestamp(columnName);
				if (timestamp != null) {
					String isoDate = DateParser.getIsoDate(timestamp);
					cell = new SimpleCell(id, isoDate);
				} else {
					cell = new SimpleCell(id, null);
				}
			}
		} else {
			Date date = rawData.getDate(columnName);
			if (date != null) {
				cell = new SimpleCell(id, date.toString());
			} else {
				cell = new SimpleCell(id, null);
			}
		}
		return cell;
	}
	
	protected Cell rawToCellSimpleTypeBinary(String id, String columnName,
			Type cellType, ResultSet rawData) 
					throws SQLException, ModuleException {
		Cell cell;
		InputStream binaryStream = rawData.getBinaryStream(columnName);
		if (binaryStream != null) {
			FileItem fileItem = new FileItem(binaryStream);
			FileFormat fileFormat = FormatUtility.getFileFormat(fileItem
					.getFile());
			List<FileFormat> formats = new ArrayList<FileFormat>();
			formats.add(fileFormat);
			cell = new BinaryCell(id, fileItem, formats);
		} else {
			cell = new BinaryCell(id);
		}
		return cell;
	}
	
	protected boolean isRowValid(ResultSet raw, TableStructure structure)
			throws InvalidDataException, SQLException {
		boolean ret;
		ResultSetMetaData metadata = raw.getMetaData();
		if (metadata.getColumnCount() == structure.getColumns().size()) {
			ret = true;
		} else {
			ret = false;
			throw new InvalidDataException("table: " + structure.getName()
					+ " row number: " + raw.getRow()
					+ " error: different column number from structure "
					+ metadata.getColumnCount() + "!="
					+ structure.getColumns().size());
		}
		return ret;
	}
	
	protected ResultSet getTableRawData(String tableId) throws SQLException,
			ClassNotFoundException, ModuleException {
		logger.debug("query: " + sqlHelper.selectTableSQL(tableId));
		ResultSet set = getStatement().executeQuery(
				sqlHelper.selectTableSQL(tableId));
		set.setFetchSize(ROW_FETCH_BLOCK_SIZE);
		return set;
	}
	
	/**
	 * Gets the schemas that won't be exported.
	 * 
	 * Accepts schemas names in as regular expressions
	 * I.e. SYS.* will ignore SYSCAT, SYSFUN, etc
	 * 
	 * @return the schemas to be ignored at export
	 */
	protected Set<String> getIgnoredExportedSchemas() {
		// no ignored schemas.
		return new HashSet<String>();
	}

	public void getDatabase(DatabaseHandler handler) throws ModuleException,
			UnknownTypeException, InvalidDataException {
		try {
			logger.debug("initializing database");
			handler.initDatabase();
			// sets schemas won't be exported
			handler.setIgnoredSchemas(getIgnoredExportedSchemas());
			logger.info("STARTED: Getting the database structure.");
			handler.handleStructure(getDatabaseStructure());
			logger.info("FINISHED: Getting the database structure.");
			// logger.debug("db struct: " + getDatabaseStructure().toString());
			for (SchemaStructure schema: getDatabaseStructure().getSchemas()) {
				for (TableStructure table : schema.getTables()) {
					logger.info("STARTED: Getting data of table: " 
							+ table.getId());
					ResultSet tableRawData = getTableRawData(table.getId());
					handler.handleDataOpenTable(table.getId());
					int nRows = 0;
					while (tableRawData.next()) {
						handler.handleDataRow(
								convertRawToRow(tableRawData, table));
						nRows++;
						if (nRows % 1000 == 0) {
							logger.info(nRows + " rows processed");
						}
					}
					logger.info("Total of " + nRows + " row(s) processed");
					getDatabaseStructure().
							lookupTableStructure(table.getId()).
							setRows(nRows);
					handler.handleDataCloseTable(table.getId());
					logger.info("FINISHED: Getting data of table: " 
							+ table.getId());
				}				
			}
			logger.debug("finishing database");
			handler.finishDatabase();
		} catch (SQLException e) {
			throw new ModuleException("SQL error while conecting", e);
		} catch (ClassNotFoundException e) {
			throw new ModuleException(
					"JDBC driver class could not be found", e);
		}
	}

}
