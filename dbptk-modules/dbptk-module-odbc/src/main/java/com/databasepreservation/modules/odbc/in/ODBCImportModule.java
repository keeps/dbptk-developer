/**
 *
 */
package com.databasepreservation.modules.odbc.in;

import com.databasepreservation.model.modules.DatatypeImporter;
import com.databasepreservation.modules.SQLHelper;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;

/**
 * @author Luis Faria
 */
public class ODBCImportModule extends JDBCImportModule {

  public ODBCImportModule(String source) {
    super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source);
  }

  public ODBCImportModule(String source, String username, String password) {
    super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source + ";UID=" + username + ";PWD=" + password);
  }

  public ODBCImportModule(String source, SQLHelper sqlHelper, DatatypeImporter datatypeImporter) {
    super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source, sqlHelper, datatypeImporter);
  }
}
