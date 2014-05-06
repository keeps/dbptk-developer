/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.postgreSql.in;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.FileFormat;
import pt.gov.dgarq.roda.common.FormatUtility;
import pt.gov.dgarq.roda.common.convert.db.model.data.BinaryCell;
import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.data.FileItem;
import pt.gov.dgarq.roda.common.convert.db.model.data.SimpleCell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.postgreSql.PostgreSQLHelper;
import pt.gov.dgarq.roda.common.convert.db.modules.siard.SIARDHelper;

/**
 * <p>
 * Module to import data from a PostgreSQL database management system via JDBC
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
public class PostgreSQLJDBCImportModule extends JDBCImportModule {
	
	private final Logger logger = 
			Logger.getLogger(PostgreSQLJDBCImportModule.class);
 

	/**
	 * Create a new PostgreSQL JDBC import module
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
	public PostgreSQLJDBCImportModule(String hostname, String database,
			String username, String password, boolean encrypt) {
		super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + "/"
				+ database + "?user=" + username + "&password=" + password
				+ (encrypt ? "&ssl=true" : ""), new PostgreSQLHelper());
	}

	/**
	 * Create a new PostgreSQL JDBC import module
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
	public PostgreSQLJDBCImportModule(String hostname, int port,
			String database, String username, String password, boolean encrypt) {
		super("org.postgresql.Driver", "jdbc:postgresql://" + hostname + ":"
				+ port + "/" + database + "?user=" + username + "&password="
				+ password + (encrypt ? "&ssl=true" : ""), 
				new PostgreSQLHelper());
	}
	
	/**
	 * Schemas that won't be exported
	 */
	public Set<String> getIgnoredSchemas() {
		Set<String> ignoredSchemas = new HashSet<String>();
		ignoredSchemas.add("information_schema");
		ignoredSchemas.add("pg_.*");
		
		return ignoredSchemas;
	}
	
	protected Type getNumericType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		if (columnSize > 131000) {
			logger.info("Numeric or decimal column with no "
					+ "precision or scale specified");
			logger.info("Lowering precision to 28 with scale 10");
			columnSize = 28;
			decimalDigits = 10;
		}
		return new SimpleTypeNumericExact(
				Integer.valueOf(columnSize), Integer.valueOf(decimalDigits));
	}
	
	protected Type getDecimalType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		if (columnSize > 131000) {
			logger.warn("Numeric or decimal column with no defined "
					+ "precision or scale");
			logger.warn("Lowering precision to 28 and scale to 10");
			columnSize = 28;
			decimalDigits = 10;
		}
		return new SimpleTypeNumericExact(
				Integer.valueOf(columnSize), Integer.valueOf(decimalDigits));
	}
	
	protected Type getDoubleType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		if (typeName.equalsIgnoreCase("MONEY") 
				|| typeName.equalsIgnoreCase("FLOAT8")) {
			columnSize = 53;
		}
		return new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
	}
	
	protected Type getTimestampType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		if (typeName.equalsIgnoreCase("TIMESTAMPTZ")) {
			return new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
		} else {
			return new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		}
	}
	
	protected Type getTimeType(String typeName, int columnSize, 
			int decimalDigits, int numPrecRadix) {
		if (typeName.equalsIgnoreCase("TIMETZ")) {
			return new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
		} else {
			return new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		}
	}
		
	protected Cell rawToCellSimpleTypeNumericApproximate(String id, 
			String columnName, Type cellType, ResultSet rawData) 
					throws SQLException {
		Cell cell = null;
		if (cellType.getOriginalTypeName().equalsIgnoreCase("MONEY")) {
			String data = rawData.getString(columnName);
			String parts[] = data.split(" ");
			if (parts[1] != null) {
				logger.warn("Money currency lost: " + parts[1]);
			}
			cell = new SimpleCell(id, parts[0]);
		}
		else {
			String value;
			if (cellType.getOriginalTypeName().equalsIgnoreCase("float4")) {
				Float f = rawData.getFloat(columnName);
				value = f.toString();
			} else {
				Double d = rawData.getDouble(columnName);
				value = d.toString();
			}
			logger.debug("VALUE: " + value);
			cell = new SimpleCell(id, value);
		}
		return cell;
	}
	
	protected Cell rawToCellSimpleTypeBinary(String id, String columnName,
			Type cellType, ResultSet rawData) 
					throws SQLException, ModuleException {
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
			FileFormat fileFormat = FormatUtility.getFileFormat(fileItem
					.getFile());
			List<FileFormat> formats = new ArrayList<FileFormat>();
			formats.add(fileFormat);
			cell = new BinaryCell(id, fileItem, formats);
		} else {
			cell = new BinaryCell(id);
		}
		return cell;
	}
	
}
