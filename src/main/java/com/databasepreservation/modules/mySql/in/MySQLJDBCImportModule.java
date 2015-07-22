/**
 *
 */
package com.databasepreservation.modules.mySql.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria
 *
 */
public class MySQLJDBCImportModule extends JDBCImportModule {

	private final Logger logger = Logger.getLogger(MySQLJDBCImportModule.class);

	/**
	 * MySQL JDBC import module constructor
	 *
	 * @param hostname
	 *            the hostname of the MySQL server
	 * @param database
	 *            the name of the database to import from
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 */
	public MySQLJDBCImportModule(String hostname, String database,
			String username, String password) {
		super("com.mysql.jdbc.Driver",
				"jdbc:mysql://" + hostname + "/" + database + "?" + "user="
						+ username + "&password=" + password, new MySQLHelper());
	}

	/**
	 * MySQL JDBC import module constructor
	 *
	 * @param hostname
	 *            the hostname of the MySQL server
	 * @param port
	 *            the port that the MySQL server is listening
	 * @param database
	 *            the name of the database to import from
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 */
	public MySQLJDBCImportModule(String hostname, int port, String database,
			String username, String password) {
		super("com.mysql.jdbc.Driver", "jdbc:mysql://" + hostname + ":" + port
				+ "/" + database + "?" + "user=" + username + "&password="
				+ password, new MySQLHelper());
	}

	@Override
	protected boolean isGetRowAvailable() {
		return false;
	}

	@Override
	protected List<SchemaStructure> getSchemas() throws SQLException,
			ClassNotFoundException, UnknownTypeException {
		List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
		String schemaName = getConnection().getCatalog();
		schemas.add(getSchemaStructure(schemaName, 1));
		return schemas;
	}

	@Override
	protected String getReferencedSchema(String s) throws SQLException,
			ClassNotFoundException {
		return (s == null) ? getConnection().getCatalog() : s;
	}

	@Override
	protected List<UserStructure> getUsers() throws SQLException,
			ClassNotFoundException {
		List<UserStructure> users = new ArrayList<UserStructure>();
		ResultSet rs = getStatement().executeQuery(sqlHelper.getUsersSQL(null));
		while (rs.next()) {
			UserStructure user = new UserStructure(rs.getString(2) + "@"
					+ rs.getString(1), null);
			users.add(user);
		}

		return users;
	}

	@Override
	protected ResultSet getTableRawData(String tableId) throws SQLException,
			ClassNotFoundException, ModuleException {
		logger.debug("query: " + sqlHelper.selectTableSQL(tableId));

		Statement statement = getStatement();
		statement.setFetchSize(Integer.MIN_VALUE);

		ResultSet set = statement.executeQuery(sqlHelper
				.selectTableSQL(tableId));
		return set;
	}

	@Override
	protected Statement getStatement() throws SQLException,
			ClassNotFoundException {
		if (statement == null) {
			statement = getConnection().createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		}
		return statement;
	}
}
