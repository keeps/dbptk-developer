package pt.gov.dgarq.roda.common.convert.db.modules.db2.out;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.modules.db2.DB2Helper;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.out.JDBCExportModule;

/**
 * @author Miguel Coutada
 *
 */
public class DB2JDBCExportModule extends JDBCExportModule {
	//FIXME remove it
	private final Logger logger = Logger.getLogger(DB2JDBCExportModule.class);

	
	/**
	 * Db2 JDBC export module constructor
	 * 
	 * @param hostname
	 * 			  the host name of Db2 Server  
	 * @param port
	 * 			  the port that the Db2 server is listening 			
	 * @param database
	 *            the name of the database to import from
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 */
	public DB2JDBCExportModule(String hostname, int port, String database,
			String username, String password) {
		super("com.ibm.db2.jcc.DB2Driver", "jdbc:db2://" + hostname 
				+ ":" + port + "/" + database + ":user=" + username 
				+ ";password=" + password + ";", new DB2Helper());

	}
		
	public void finishDatabase() throws ModuleException {
		if (databaseStructure != null) {
			logger.info("Handling foreign keys is not yet supported!");
			//handleForeignKeys();
			commit();
		}
	}
	
}
