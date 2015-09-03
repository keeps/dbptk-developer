/**
 *
 */
package com.databasepreservation.modules.postgreSql.in;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.postgreSql.PostgreSQLHelper;
import com.databasepreservation.modules.siard.SIARDHelper;
import org.apache.log4j.Logger;
import org.postgresql.util.PGobject;
import pt.gov.dgarq.roda.common.FileFormat;
import pt.gov.dgarq.roda.common.FormatUtility;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>
 * Module to import data from a PostgreSQL database management system via JDBC
 * driver. The postgresql-8.3-603.jdbc3.jar driver supports PostgreSQL version
 * 7.4 to 8.3.
 * </p>
 * <p>
 * <p>
 * To use this module, the PostgreSQL server must be configured:
 * </p>
 * <ol>
 * <li>Server must be configured to accept TCP/IP connections. This can be done
 * by setting <code>listen_addresses = 'localhost'</code> (or
 * <code>tcpip_socket = true</code> in older versions) in the postgresql.conf
 * file.</li>
 * <li>The client authentication setup in the pg_hba.conf file may need to be
 * configured, adding a line like
 * <code>host all all 127.0.0.1 255.0.0.0 trust</code>. The JDBC driver supports
 * the trust, ident, password, md5, and crypt authentication methods.</li>
 * </ol>
 *
 * @author Luis Faria
 */
public class PostgreSQLJDBCImportModule extends JDBCImportModule {

        private final Logger logger = Logger.getLogger(PostgreSQLJDBCImportModule.class);

        /**
         * Create a new PostgreSQL JDBC import module
         *
         * @param hostname the name of the PostgreSQL server host (e.g. localhost)
         * @param database the name of the database to connect to
         * @param username the name of the user to use in connection
         * @param password the password of the user to use in connection
         * @param encrypt  encrypt connection
         */
        public PostgreSQLJDBCImportModule(String hostname, String database, String username, String password,
          boolean encrypt) {
                super("org.postgresql.Driver",
                  "jdbc:postgresql://" + hostname + "/" + database + "?user=" + username + "&password=" + password + (
                    encrypt ?
                      "&ssl=true" :
                      ""), new PostgreSQLHelper());
        }

        /**
         * Create a new PostgreSQL JDBC import module
         *
         * @param hostname the name of the PostgreSQL server host (e.g. localhost)
         * @param port     the port of where the PostgreSQL server is listening, default
         *                 is 5432
         * @param database the name of the database to connect to
         * @param username the name of the user to use in connection
         * @param password the password of the user to use in connection
         * @param encrypt  encrypt connection
         */
        public PostgreSQLJDBCImportModule(String hostname, int port, String database, String username, String password,
          boolean encrypt) {
                super("org.postgresql.Driver",
                  "jdbc:postgresql://" + hostname + ":" + port + "/" + database + "?user=" + username + "&password="
                    + password + (encrypt ? "&ssl=true" : ""), new PostgreSQLHelper());
        }

        @Override public Connection getConnection() throws SQLException, ClassNotFoundException {
                if (connection == null) {
                        logger.debug("Loading JDBC Driver " + driverClassName);
                        Class.forName(driverClassName);
                        logger.debug("Getting connection");
                        connection = DriverManager.getConnection(connectionURL);
                        connection.setAutoCommit(false);
                        logger.debug("Connected");

                }
                return connection;
        }

        @Override protected Statement getStatement() throws SQLException, ClassNotFoundException {
                if (statement == null) {
                        statement = getConnection().createStatement();
                }
                return statement;
        }

        @Override protected ResultSet getTableRawData(String tableId)
          throws SQLException, ClassNotFoundException, ModuleException {
                logger.debug("query: " + sqlHelper.selectTableSQL(tableId));
                Statement st = getStatement();
                st.setFetchSize(200);
                ResultSet set = st.executeQuery(sqlHelper.selectTableSQL(tableId));
                return set;
        }

        /**
         * Gets the schemas that won't be exported. Defaults to PostgreSQL are
         * information_schema and all pg_XXX
         */
        @Override public Set<String> getIgnoredExportedSchemas() {
                Set<String> ignoredSchemas = new HashSet<String>();
                ignoredSchemas.add("information_schema");
                ignoredSchemas.add("pg_.*"); //TODO: is this working?

                return ignoredSchemas;
        }

        @Override protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
                Type type = new SimpleTypeBinary(columnSize);
                if (typeName.equalsIgnoreCase("bytea")) {
                        type.setSql99TypeName("BINARY LARGE OBJECT");
                } else {
                        type.setSql99TypeName("BIT");
                }
                return type;
        }

        @Override protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
                if (typeName.equalsIgnoreCase("MONEY") || typeName.equalsIgnoreCase("FLOAT8")) {
                        logger.warn("Setting Money column size to 53");
                        columnSize = 53;
                }
                Type type = new SimpleTypeNumericApproximate(columnSize);
                type.setSql99TypeName("DOUBLE PRECISION");
                return type;
        }

        @Override protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
                Type type;
                if (typeName.equalsIgnoreCase("TIMETZ")) {
                        type = new SimpleTypeDateTime(true, true);
                        type.setSql99TypeName("TIME WITH TIME ZONE");
                } else {
                        type = new SimpleTypeDateTime(true, false);
                        type.setSql99TypeName("TIME");
                }

                return type;
        }

        @Override protected Type getTimestampType(String typeName, int columnSize, int decimalDigits,
          int numPrecRadix) {
                Type type;
                if (typeName.equalsIgnoreCase("TIMESTAMPTZ")) {
                        type = new SimpleTypeDateTime(true, true);
                        type.setSql99TypeName("TIMESTAMP WITH TIME ZONE");
                } else {
                        type = new SimpleTypeDateTime(true, false);
                        type.setSql99TypeName("TIMESTAMP");
                }

                return type;
        }

        @Override protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
                Type type = new SimpleTypeString(columnSize, true);
                if (typeName.equalsIgnoreCase("text")) {
                        type.setSql99TypeName("CHARACTER LARGE OBJECT");
                } else {
                        type.setSql99TypeName("CHARACTER VARYING");
                }
                return type;
        }

        @Override protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits,
          int numPrecRadix) throws UnknownTypeException {
                Type type;
                logger.debug("Specific type name: " + typeName);
                logger.debug("------\n");
                switch (dataType) {
                        case 2009: // XML Data type
                                type = new SimpleTypeString(columnSize, true);
                                type.setSql99TypeName("CHARACTER LARGE OBJECT");
                                break;
                        default:
                                type = super
                                  .getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
                                break;
                }
                return type;
        }

        /**
         * Drops money currency
         */
        @Override protected Cell rawToCellSimpleTypeNumericApproximate(String id, String columnName, Type cellType,
          ResultSet rawData) throws SQLException {
                Cell cell = null;
                if (cellType.getOriginalTypeName().equalsIgnoreCase("MONEY")) {
                        String data = rawData.getString(columnName);
                        if (data != null) {
                                String parts[] = data.split(" ");
                                if (parts[1] != null) {
                                        logger.warn("Money currency lost: " + parts[1]);
                                }
                                cell = new SimpleCell(id, parts[0]);
                        } else {
                                cell = new SimpleCell(id, null);
                        }

                } else {
                        String value;
                        if (cellType.getOriginalTypeName().equalsIgnoreCase("float4")) {
                                Float f = rawData.getFloat(columnName);
                                value = rawData.wasNull() ? null : f.toString();
                        } else {
                                Double d = rawData.getDouble(columnName);
                                value = rawData.wasNull() ? null : d.toString();
                        }
                        cell = new SimpleCell(id, value);
                }
                return cell;
        }

        /**
         * Treats bit strings, as the default behavior does not handle PostgreSQL
         * byte streams correctly
         */
        @Override protected Cell rawToCellSimpleTypeBinary(String id, String columnName, Type cellType,
          ResultSet rawData) throws SQLException, ModuleException {
                Cell cell;
                InputStream binaryStream;
                if (cellType.getOriginalTypeName().equalsIgnoreCase("bit")) {
                        String bitString = rawData.getString(columnName);
                        String hexString = new BigInteger(bitString, 2).toString(16);
                        if ((hexString.length() % 2) != 0) {
                                hexString = "0" + hexString;
                        }
                        byte[] bytes = SIARDHelper.hexStringToByteArray(hexString);
                        binaryStream = new ByteArrayInputStream(bytes);
                } else {
                        binaryStream = rawData.getBinaryStream(columnName);
                }
                if (binaryStream != null) {
                        FileItem fileItem = new FileItem(binaryStream);
                        FileFormat fileFormat = FormatUtility.getFileFormat(fileItem.getFile());
                        List<FileFormat> formats = new ArrayList<FileFormat>();
                        formats.add(fileFormat);
                        cell = new BinaryCell(id, fileItem, formats);
                } else {
                        cell = new BinaryCell(id);
                }
                return cell;
        }

        @Override protected Cell rawToCellSimpleTypeDateTime(String id, String columnName, Type cellType,
          ResultSet rawData) throws SQLException {
                Cell cell = null;
                SimpleTypeDateTime undefinedDate = (SimpleTypeDateTime) cellType;
                if (undefinedDate.getTimeDefined()) {
                        if (cellType.getSql99TypeName().equalsIgnoreCase("TIME WITH TIME ZONE")) {
                                String time_string = rawData.getString(columnName);
                                if (time_string.matches("^\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}$")) {
                                        cell = new SimpleCell(id, time_string + ":00");
                                        logger.trace(
                                          "rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpledata()));
                                } else if (time_string.matches("^\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{2}:\\d{2}$")) {
                                        cell = new SimpleCell(id, time_string);
                                        logger.trace(
                                          "rawToCellSimpleTypeDateTime cell: " + (((SimpleCell) cell).getSimpledata()));
                                } else {
                                        cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
                                }
                        } else {
                                cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
                        }
                } else {
                        cell = super.rawToCellSimpleTypeDateTime(id, columnName, cellType, rawData);
                }
                return cell;
        }

        class PGTime extends PGobject {

                /**
                 *
                 */
                private static final long serialVersionUID = 1L;

        }

}
