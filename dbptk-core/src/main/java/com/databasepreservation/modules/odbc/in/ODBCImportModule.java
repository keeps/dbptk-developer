/**
 *
 */
package com.databasepreservation.modules.odbc.in;

import com.databasepreservation.modules.SQLHelper;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;

/**
 * @author Luis Faria
 */
public class ODBCImportModule extends JDBCImportModule {

        /**
         * @param source
         */
        public ODBCImportModule(String source) {
                super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source);
        }

        public ODBCImportModule(String source, String username, String password) {
                super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source + ";UID=" + username + ";PWD=" + password);
        }

        /**
         * @param source
         * @param sqlHelper
         */
        public ODBCImportModule(String source, SQLHelper sqlHelper) {
                super("sun.jdbc.odbc.JdbcOdbcDriver", "jdbc:odbc:" + source, sqlHelper);
        }
}
