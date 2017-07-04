package com.databasepreservation.modules.oracle.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.oracle.OracleHelper;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria <lfaria@keep.pt>
 */
public class Oracle12cJDBCImportModule extends JDBCImportModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(Oracle12cJDBCImportModule.class);

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
  public Oracle12cJDBCImportModule(String serverName, int port, String database, String username, String password) {

    super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username + "/" + password + "@//" + serverName + ":"
      + port + "/" + database, new OracleHelper(), new Oracle12cJDBCDatatypeImporter());

    LOGGER.debug("jdbc:oracle:thin:<username>/<password>@//" + serverName + ":" + port + "/" + database);
  }

  @Override
  protected Statement getStatement() throws SQLException {
    if (statement == null || statement.isClosed()) {
      statement = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }
    return statement;
  }

  @Override
  protected String getDbName() throws SQLException {
    return getMetadata().getUserName();
  }

  @Override
  protected List<SchemaStructure> getSchemas() throws SQLException {
    List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
    String schemaName = getMetadata().getUserName();
    schemas.add(getSchemaStructure(schemaName, 1));
    return schemas;
  }

  @Override
  protected String processActionTime(String string) {
    String[] parts = string.split("\\s+");
    String res = parts[0];
    if ("INSTEAD".equalsIgnoreCase(res)) {
      res += " OF";
    }
    return res;
  }
}
