package pt.gov.dgarq.roda.common.convert.db.modules.msAccess.in;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.msAccess.MsAccessHelper;

public class MsAccessUCanAccessImportModule extends JDBCImportModule {
	
	private final Logger logger = 
			Logger.getLogger(MsAccessUCanAccessImportModule.class);
	
	public MsAccessUCanAccessImportModule(File msAccessFile) {
		super ("net.ucanaccess.jdbc.UcanaccessDriver", "jdbc:ucanaccess://" 
				+ msAccessFile.getAbsolutePath() + ";showSchema=true;", 
				new MsAccessHelper());
	}
	
	public Connection getConnection() throws SQLException, 
			ClassNotFoundException {
		if (connection == null) {
			logger.debug("Loading JDBC Driver " + driverClassName);
			Class.forName(driverClassName);
			logger.debug("Getting connection");
			connection = DriverManager
					.getConnection(connectionURL); //, "admin", "admin");
			logger.debug("Connected");
		}
		return connection;
	}
	
	
	protected ResultSet getTableRawData(String tableId) throws SQLException,
	ClassNotFoundException, ModuleException {
		String tableName;
		ResultSet set = null;
		try {
			tableName = getDatabaseStructure().lookupTableStructure(tableId).
					getName();
			logger.debug("query: " + sqlHelper.selectTableSQL(tableName));
			set = getStatement().executeQuery(
					sqlHelper.selectTableSQL(tableName));
			set.setFetchSize(ROW_FETCH_BLOCK_SIZE);
		} catch (UnknownTypeException e) {
			logger.debug("");
		}
		
		return set;
	}
	
	/**
	 * Gets the schemas that won't be imported. 
	 * Defaults to MsAccess are all INFORMATION_SCHEMA_XX
	 * 
	 * @return the schemas to be ignored at import 
	 */
	
	@Override
	protected Set<String> getIgnoredImportedSchemas() {
		Set<String> ignoredSchemas = new HashSet<String>();
		ignoredSchemas.add("INFORMATION_SCHEMA.*");
		
		return ignoredSchemas;
	}
}
