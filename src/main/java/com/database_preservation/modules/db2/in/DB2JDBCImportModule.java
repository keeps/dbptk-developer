package com.database_preservation.modules.db2.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import com.database_preservation.model.exception.UnknownTypeException;
import com.database_preservation.model.structure.DatabaseStructure;
import com.database_preservation.model.structure.ForeignKey;
import com.database_preservation.model.structure.type.SimpleTypeNumericApproximate;
import com.database_preservation.model.structure.type.SimpleTypeString;
import com.database_preservation.model.structure.type.Type;
import com.database_preservation.modules.db2.DB2Helper;
import com.database_preservation.modules.jdbc.in.JDBCImportModule;

/** 
 * @author Miguel Coutada
 *
 */
public class DB2JDBCImportModule extends JDBCImportModule {
	
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
				+ ";password=" + password + ";", new DB2Helper());
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
	
	/**
	 * Returns the default ignored schemas for DB2
	 * These schemas won't be imported
	 */
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
	protected Type getOtherType(int dataType, String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) throws UnknownTypeException {
		Type type;
		if (typeName.equalsIgnoreCase("XML")) {
			type = new SimpleTypeString(31457280, Boolean.TRUE);
			type.setSql99TypeName("CHARACTER LARGE OBJECT");
		} else if (typeName.equalsIgnoreCase("DECFLOAT")) {
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("DOUBLE");
		} else {		
			type = super.getOtherType(dataType, typeName, columnSize, 
					decimalDigits, numPrecRadix);
		}
		return type; 
	}
	
	@Override
	protected Type getSpecificType(int dataType, String typeName, 
			int columnSize, int decimalDigits, int numPrecRadix) 
					throws UnknownTypeException {
		Type type;
		switch (dataType) {
//		case 2001:
//			type = new SimpleTypeNumericApproximate(
//					Integer.valueOf(columnSize));
//			break;
		default:
			type = super.getSpecificType(dataType, typeName, columnSize, 
					decimalDigits, numPrecRadix);
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
	// VERIFY Need of custom getForeignKeys
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

	@Override
	protected String processTriggerEvent(String string) {
		String res = "";
		if (string.equals("I")) {
			res = "INSERT";
		} else if (string.equals("U")) {
			res = "UPDATE";
		} else if (string.equals("D")) {
			res = "DELETE";
		}
		return res;
	}

	@Override
	protected String processActionTime(String string) {
		String res = "";
		if (string.equals("B")) {
			res = "BEFORE";
		} else if (string.equals("A")) {
			res = "AFTER";
		} else if (string.equals("I")) {
			res = "INSTEAD OF";
		}
		return res;
	}
}
