package pt.gov.dgarq.roda.common.convert.db.modules.db2.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ForeignKey;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;

/** 
 * @author Miguel Coutada
 *
 */
public class DB2JDBCImportModule extends JDBCImportModule {
	private final Logger logger = Logger.getLogger(DB2JDBCImportModule.class);
	
	private List<String> aliasTables = null;
	
	private String dbName;
	
	
	/**
	 * Db2 JDBC import module constructor
	 * 
	 * @param hostname
	 *            the hostname of the Db2 server
	 * @param port 
	 *            the port that the Db2 server is listening
	 * @param database
	 *            the name of the database to import from	
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 */
	public DB2JDBCImportModule(String hostname, int port, String database, 
			String username, String password) {
		super("com.ibm.db2.jcc.DB2Driver", "jdbc:db2://" + hostname 
				+ ":" + port + "/" + database + ":user=" + username 
				+ ";password=" + password + ";");
		dbName = database;
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
			dbStructure = super.getDatabaseStructure();
			dbStructure.setName(dbName);
		}
		return dbStructure;
	}
	
	protected Set<String> getIgnoredImportedSchemas() {
		Set<String> ignored = new HashSet<String>();
		ignored.add("SQLJ");
		ignored.add("NULLID");
		ignored.add("SYSCAT");
		ignored.add("SYSFUN");
		ignored.add("SYSIBM");
		ignored.add("SYSIBMADM");
		ignored.add("SYSIBMINTERNAL");
		ignored.add("SYSIBMTS");
		ignored.add("SYSPROC");
		ignored.add("SYSPUBLIC");
		ignored.add("SYSSTAT");
		ignored.add("SYSTOOLS");
		return ignored;
	}
	
	@Override
	protected Type getOtherType(int dataType, String typeName, int columnSize)
			throws UnknownTypeException {
		Type type;
		logger.debug("TYPE NAME: " + typeName);
		logger.debug("-----\n");
		if (typeName.equalsIgnoreCase("XML")) {
			type = new SimpleTypeString(31457280, Boolean.TRUE);
			type.setSql99TypeName("CHARACTER LARGE OBJECT");
		} else if (typeName.equalsIgnoreCase("DECFLOAT")) {
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("DOUBLE");
		} else {		
			type = super.getOtherType(dataType, typeName, columnSize);
		}
		return type; 
	}
	
	@Override
	protected Type getSpecificType(int dataType, String typeName, 
			int columnSize) throws UnknownTypeException {
		Type type;
		logger.debug("Specific type name: " + typeName);
		logger.debug("------\n");
		switch (dataType) {
//		case 2001:
//			type = new SimpleTypeNumericApproximate(
//					Integer.valueOf(columnSize));
//			break;
		default:
			type = super.getSpecificType(dataType, typeName, columnSize);
			break;
		}
		return type;
	}

	/**
	 * 
	 * @return the db2 database alias tables
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	protected List<String> getAliasTables() 
			throws SQLException, ClassNotFoundException {
		List<String> aliasTables = new ArrayList<String>();
		
		ResultSet rset = getMetadata().getTables(dbStructure.getName(), 
				null, "%", new String[] { "ALIAS" });
		while(rset.next()) {
			aliasTables.add(rset.getString("TABLE_NAME"));
		}
		return aliasTables;
	}
	
	/**
	 * @param tableName
	 * 			the table name
	 * @return the foreign keys
	 * @throws SQLException
	 * @throws UnknownTypeException
	 * @throws ClassNotFoundException 
	 */
	protected List<ForeignKey> getForeignKeys(String tableName)
			throws SQLException, UnknownTypeException, ClassNotFoundException {
		List<ForeignKey> foreignKeys = new Vector<ForeignKey>();
		if (aliasTables == null) {
			aliasTables = getAliasTables();
		}
		
		ResultSet rs = getMetadata().getImportedKeys(
				getDatabaseStructure().getName(), null, tableName);
		
		while (rs.next()) {
			String name = rs.getString(8);
			String refTable = rs.getString(3);
			String refColumn = rs.getString(4);
			if (!aliasTables.contains(refTable)) {
				ForeignKey fk = new ForeignKey(tableName + "." + name, name,
						refTable, refColumn);
				foreignKeys.add(fk);
			}
		}
		return foreignKeys;
	}
	
}
