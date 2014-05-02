/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.mySql.out;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.util.InvalidDateException;

import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.out.JDBCExportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class MySQLJDBCExportModule extends JDBCExportModule {

	protected static final String MYSQL_ADMIN_DATABASE = "mysql";

	private final Logger logger = Logger.getLogger(MySQLJDBCExportModule.class);

	private final String hostname;

	private final int port;

	private final String username;

	private final String password;

	private final Map<String, Connection> connections;
	
	private static final String[] IGNORED_SCHEMAS = 
			{ "mysql", "performance_schema", "information_schema" };

	/**
	 * MySQL JDBC export module constructor
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
	public MySQLJDBCExportModule(String hostname, String database,
			String username, String password) {
		super("com.mysql.jdbc.Driver",
				"jdbc:mysql://" + hostname + "/" + database + "?" + "user="
						+ username + "&password=" + password, 
						new MySQLHelper());
		this.hostname = hostname;
		this.port = -1;
		this.username = username;
		this.password = password;
		this.connections = new HashMap<String, Connection>();
		super.ignoredSchemas = 
				new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
	}

	/**
	 * MySQL JDBC export module constructor
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
	public MySQLJDBCExportModule(String hostname, int port, String database,
			String username, String password) {
		super("com.mysql.jdbc.Driver", "jdbc:mysql://" + hostname + ":" + port
				+ "/" + database + "?" + "user=" + username + "&password="
				+ password, new MySQLHelper());
		this.hostname = hostname;
		this.port = port;
		this.username = username;
		this.password = password;
		this.connections = new HashMap<String, Connection>();
		super.ignoredSchemas = 
				new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
	}

	/**
	 * Get a connection to a database. This connection can be used to create the
	 * database
	 * 
	 * @param databaseName
	 *            the name of the database to connect
	 * 
	 * @return the JDBC connection
	 * @throws ModuleException
	 */
	public Connection getConnection(String databaseName) throws ModuleException {
		Connection connection;
		if (!connections.containsKey(databaseName)) {
			String connectionURL = "jdbc:mysql://" + hostname
					+ (port >= 0 ? ":" + port : "") + "/" + databaseName + "?"
					+ "user=" + username + "&password=" + password;
			try {
				logger.debug("Loading JDBC Driver " + driverClassName);
				Class.forName(driverClassName);
				logger.debug("Getting admin connection");
				connection = DriverManager.getConnection(connectionURL);
				connection.setAutoCommit(true);
				logger.debug("Connected");
				connections.put(databaseName, connection);
			} catch (ClassNotFoundException e) {
				throw new ModuleException(
						"JDBC driver class could not be found", e);
			} catch (SQLException e) {
				throw new ModuleException("SQL error creating connection", e);
			}

		} else {
			connection = connections.get(databaseName);
		}
		return connection;
	}
	
	protected List<String> getExistingSchemasNames() 
			throws SQLException, ModuleException {
		List<String> existingSchemas = new ArrayList<String>();
		ResultSet rs = getConnection().getMetaData().getCatalogs();
		while (rs.next()) {
			existingSchemas.add(rs.getString(1));
		}
		return existingSchemas;
	}

	
	protected void handleSimpleTypeDateTimeDataCell(String data,
			PreparedStatement ps, int index, Cell cell, Type type) 
					throws InvalidDateException, SQLException {
		SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
		if (dateTime.getTimeDefined()) {
			if (StringUtils.startsWithIgnoreCase(type.getOriginalTypeName(),
					"TIMESTAMP") || StringUtils.startsWithIgnoreCase(
							type.getOriginalTypeName(), "DATETIME")) {
				if (data != null) {
					logger.debug("timestamp before: " + data);
					Calendar cal = javax.xml.bind.DatatypeConverter.
							parseDateTime(data);
					Timestamp sqlTimestamp = 
							new Timestamp(cal.getTimeInMillis());
					logger.debug("timestamp after: " + sqlTimestamp.toString());
					ps.setTimestamp(index, sqlTimestamp);
				} else {
					ps.setNull(index, Types.TIMESTAMP);
				}
			} else {
				if (data != null) {
					logger.debug("TIME before: "+ data);
					Time sqlTime = Time.valueOf(data);
					logger.debug("TIME after: "+ sqlTime.toString());
					ps.setTime(index, sqlTime);
				} else {
					ps.setNull(index, Types.TIME);
				}
			}
		} else {
			if (data != null) {
				logger.debug("DATE before: " + data);
				java.sql.Date sqlDate = java.sql.Date.valueOf(data);
				logger.debug("DATE after: " + sqlDate.toString());
				ps.setDate(index, sqlDate);
			} else {
				ps.setNull(index, Types.DATE);
			}
		}
	}
	
}
