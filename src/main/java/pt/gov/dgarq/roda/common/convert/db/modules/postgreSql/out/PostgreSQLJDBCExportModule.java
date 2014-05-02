package pt.gov.dgarq.roda.common.convert.db.modules.postgreSql.out;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.util.InvalidDateException;

import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.out.JDBCExportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.postgreSql.PostgreSQLHelper;

/**
 * <p>
 * Module to export data to a PostgreSQL database management system via JDBC
 * driver. The postgresql-8.3-603.jdbc3.jar driver supports PostgreSQL version
 * 7.4 to 8.3.
 * </p>
 * 
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
 * <code>host all all 127.0.0.1 255.0.0.0 trust</code>. The JDBC driver
 * supports the trust, ident, password, md5, and crypt authentication methods.
 * </li>
 * </ol>
 * 
 * @author Luis Faria
 * 
 */
public class PostgreSQLJDBCExportModule extends JDBCExportModule {

	private final Logger logger = 
			Logger.getLogger(PostgreSQLJDBCExportModule.class);
	
	/**
	 * Create a new PostgreSQL JDBC export module
	 * 
	 * @param hostname
	 *            the name of the PostgreSQL server host (e.g. localhost)
	 * @param database
	 *            the name of the database to connect to
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 * @param encrypt
	 *            encrypt connection
	 */
	public PostgreSQLJDBCExportModule(String hostname, String database,
			String username, String password, boolean encrypt) {
		super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + "/"
				+ database + "?user=" + username + "&password=" + password
				+ (encrypt ? "&ssl=true" : ""), new PostgreSQLHelper());
	}

	/**
	 * Create a new PostgreSQL JDBC export module
	 * 
	 * @param hostname
	 *            the name of the PostgreSQL server host (e.g. localhost)
	 * @param port
	 *            the port of where the PostgreSQL server is listening, default
	 *            is 5432
	 * @param database
	 *            the name of the database to connect to
	 * @param username
	 *            the name of the user to use in connection
	 * @param password
	 *            the password of the user to use in connection
	 * @param encrypt
	 *            encrypt connection
	 */
	public PostgreSQLJDBCExportModule(String hostname, int port,
			String database, String username, String password, boolean encrypt) {
		super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + ":"
				+ port + "/" + database + "?user=" + username + "&password="
				+ password + (encrypt ? "&ssl=true" : ""),
				new PostgreSQLHelper());
	}

	public void handleDataCloseTable(String tableId) throws ModuleException {
		try {
			logger.debug("table ID: " + currentTableStructure.getId());
			getStatement().executeUpdate(
					((PostgreSQLHelper) getSqlHelper()).grantPermissionsSQL(
							currentTableStructure.getId()));
		} catch (SQLException e) {
			throw new ModuleException(
					"Error granting permissions to public", e);
		}
		super.handleDataCloseTable(tableId);
	}

//	protected void handleSimpleTypeDateTimeDataCell(String data,
//			PreparedStatement ps, int index, Cell cell, Type type) 
//					throws InvalidDateException, SQLException {
//		SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
//		if (dateTime.getTimeDefined()) {
//			if (StringUtils.startsWithIgnoreCase(type.getOriginalTypeName(),
//					"TIMESTAMP")) {
//				if (data != null) {
//					logger.debug("set timestamp");
//					
//					SimpleDateFormat sdf = new SimpleDateFormat(
//							"yyyy-MM-dd hh:mm:ss");
//					java.util.Date date;
//					Timestamp sqlTimestamp = null;
//					try {
//						date = sdf.parse(data);
//						logger.debug("timestamp: " + date.getTime());
//						sqlTimestamp = new java.sql.Timestamp(date.getTime());
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//					// Timestamp sqlTimestamp = Timestamp.valueOf(formatted);
//					ps.setTimestamp(index, sqlTimestamp);
//				} else {
//					ps.setNull(index, Types.TIMESTAMP);
//				}
//			} else {
//				if (data != null) {
//					logger.debug("set TIME");
//					Time sqlTime = Time.valueOf(data);
//					ps.setTime(index, sqlTime);
//				} else {
//					ps.setNull(index, Types.TIME);
//				}
//			}
//		} else {
//			// Date date = DateParser.parse(data);
//			// java.sql.Date sqlDate = new java.sql.Date(date.getTime());
//			if (data != null) {
//				logger.debug("set DATE");
//				java.sql.Date sqlDate = java.sql.Date.valueOf(data);
//				ps.setDate(index, sqlDate);
//			} else {
//				ps.setNull(index, Types.DATE);
//			}
//		}
//	}
	
	protected void handleSimpleTypeDateTimeDataCell(String data,
			PreparedStatement ps, int index, Cell cell, Type type) 
					throws InvalidDateException, SQLException {
		SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
		if (dateTime.getTimeDefined()) {
			if (StringUtils.startsWithIgnoreCase(type.getOriginalTypeName(),
					"TIMESTAMP") || StringUtils.startsWithIgnoreCase(
							type.getOriginalTypeName(), "DATETIME")) {
				if (data != null) {
					Calendar cal = javax.xml.bind.DatatypeConverter.
							parseDateTime(data);
					Timestamp sqlTimestamp = 
							new Timestamp(cal.getTimeInMillis());
					ps.setTimestamp(index, sqlTimestamp);
				} else {
					ps.setNull(index, Types.TIMESTAMP);
				}
			} else {
				if (data != null) {
					Time sqlTime = Time.valueOf(data);
					ps.setTime(index, sqlTime);
				} else {
					ps.setNull(index, Types.TIME);
				}
			}
		} else {
			if (data != null) {
				java.sql.Date sqlDate = java.sql.Date.valueOf(data);
				ps.setDate(index, sqlDate);
			} else {
				ps.setNull(index, Types.DATE);
			}
		}
	}
	
	protected void handleSimpleTypeNumericApproximateDataCell(String data,
			PreparedStatement ps, int index, Cell cell, Type type) 
					throws NumberFormatException, SQLException {
		if (data != null) {
			logger.debug("set approx: " + data);
			if (type.getOriginalTypeName().equalsIgnoreCase("float4")) {
				ps.setFloat(index, Float.valueOf(data));
			} else {
				ps.setDouble(index, Double.valueOf(data));
			}
		} else {
			ps.setNull(index, Types.FLOAT);
		}		
	}
}
