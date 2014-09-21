/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.mySql.out;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.out.JDBCExportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.mySql.MySQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class MySQLJDBCExportModule extends JDBCExportModule {

	protected static final String MYSQL_CONNECTION_DATABASE = "mysql";
	
//	private final Logger logger = Logger.getLogger(MySQLJDBCExportModule.class);

	protected final String hostname;
	
	protected final String database;
	
	protected final int port;

	protected final String username;

	protected final String password;
	
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
		super("com.mysql.jdbc.Driver", createConnectionURL(hostname, -1, 
				database, username, password), new MySQLHelper());
		this.hostname = hostname;
		this.port = -1;
		this.database = database;
		this.username = username;
		this.password = password;
		this.replacedPrefix = database;
		this.ignoredSchemas = 
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
		super("com.mysql.jdbc.Driver", createConnectionURL(hostname, port, 
				database, username, password), new MySQLHelper());
		this.hostname = hostname;
		this.port = port;
		this.database = database;
		this.username = username;
		this.password = password;
		this.replacedPrefix = database;
		this.ignoredSchemas = 
				new TreeSet<String>(Arrays.asList(IGNORED_SCHEMAS));
	}
	
	// TODO add initDatabase/control the way new schemas/databases are created!
	
	public static String createConnectionURL(String hostname, int port, 
			String database, String username, String password) {
		return "jdbc:mysql://" + hostname + ":" + (port >= 0 ? ":" + port : "") 
				+ "/" + database + "?" + "user=" + username + "&password=" 
				+ password + "&rewriteBatchedStatements=true";
	}
	
	public String createConnectionURL(String databaseName) {
		return createConnectionURL(hostname, port, databaseName, 
				username, password);
	}
	
	protected Set<String> getExistingSchemasNames() 
			throws SQLException, ModuleException {
		if (existingSchemas == null) {
			existingSchemas = new HashSet<String>();
 			ResultSet rs = getConnection().getMetaData().getCatalogs();
			while (rs.next()) {
				existingSchemas.add(rs.getString(1));
			}
		}
		return existingSchemas;
	}
}
