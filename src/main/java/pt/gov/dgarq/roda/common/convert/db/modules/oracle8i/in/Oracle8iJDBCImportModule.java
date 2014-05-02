package pt.gov.dgarq.roda.common.convert.db.modules.oracle8i.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.SchemaStructure;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;

/**
 * Microsoft SQL Server JDBC import module.
 * 
 * @author Luis Faria
 */
public class Oracle8iJDBCImportModule extends JDBCImportModule {

	private final Logger logger = Logger
			.getLogger(Oracle8iJDBCImportModule.class);

	/**
	 * Create a new Oracle8i import module 
	 * 
	 * @param serverName
	 *            the name (host name) of the server
	 * @param database
	 *            the name of the database we'll be accessing
	 * @param username
	 *            the name of the user to use in the connection
	 * @param password
	 *            the password of the user to use in the connection
	 */
	public Oracle8iJDBCImportModule(String serverName, int port,
			String database, String username, String password) {
//		super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username
//				+ "/" + password + "@" + serverName + ":" + port + ":" 
//				+ database);
		
		// TODO create oracle12c
		super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username
				+ "/" + password + "@" + serverName + ":" + port + "/" 
				+ database);

		logger.info("jdbc:oracle:thin:@//" 
				+ serverName + ":" + port + "/" + database);
	}
	
	protected Statement getStatement() throws SQLException,
			ClassNotFoundException {
		if (statement == null) {
			statement = getConnection().createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
					ResultSet.HOLD_CURSORS_OVER_COMMIT);
		}
		return statement;
	}
	
	protected String getDbName() throws SQLException, ClassNotFoundException {
		return getMetadata().getUserName();
	}
	
	protected List<SchemaStructure> getSchemas() throws SQLException, 
			ClassNotFoundException, UnknownTypeException {
		List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
		String schemaName = getMetadata().getUserName();
		schemas.add(getSchemaStructure(schemaName));
		return schemas;
	}

}
