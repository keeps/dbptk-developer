package com.databasepreservation.modules.oracle.out;

import org.apache.log4j.Logger;

import com.databasepreservation.modules.jdbc.out.JDBCExportModule;
import com.databasepreservation.modules.oracle.OracleHelper;

/**
 * @author Miguel Coutada
 */

public class Oracle12cJDBCExportModule extends JDBCExportModule {

  private final Logger logger = Logger.getLogger(Oracle12cJDBCExportModule.class);

  /**
   * Create a new Oracle12c import module
   *
   * @param serverName
   *          the name (host name) of the server
   * @param database
   *          the name of the database we'll be accessing
   * @param username
   *          the name of the user to use in the connection
   * @param password
   *          the password of the user to use in the connection
   */
  public Oracle12cJDBCExportModule(String serverName, int port, String database, String username, String password) {
    super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username + "/" + password + "@" + serverName + ":"
      + port + "/" + database, new OracleHelper());

    logger.info("jdbc:oracle:thin:@//" + serverName + ":" + port + "/" + database);
  }
}
