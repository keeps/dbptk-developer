package com.databasepreservation.modules.oracle.in;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import oracle.jdbc.OracleTypes;

import org.apache.log4j.Logger;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.oracle.OracleHelper;

/**
 * Microsoft SQL Server JDBC import module.
 *
 * @author Luis Faria
 */
public class Oracle12cJDBCImportModule extends JDBCImportModule {

	private final Logger logger = Logger
			.getLogger(Oracle12cJDBCImportModule.class);

	/**
	 * Create a new Oracle12c import module
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
	public Oracle12cJDBCImportModule(String serverName, int port,
			String database, String username, String password) {

		super("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:" + username
				+ "/" + password + "@" + serverName + ":" + port + "/"
				+ database, new OracleHelper());

		logger.info("jdbc:oracle:thin:@//"
				+ serverName + ":" + port + "/" + database);
	}

	@Override
	protected Statement getStatement() throws SQLException,
			ClassNotFoundException {
		if (statement == null) {
			statement = getConnection().createStatement(
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
					ResultSet.HOLD_CURSORS_OVER_COMMIT);
		}
		return statement;
	}

	@Override
	protected String getDbName() throws SQLException, ClassNotFoundException {
		return getMetadata().getUserName();
	}

	@Override
	protected List<SchemaStructure> getSchemas() throws SQLException,
			ClassNotFoundException, UnknownTypeException {
		List<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
		String schemaName = getMetadata().getUserName();
		schemas.add(getSchemaStructure(schemaName, 1));
		return schemas;
	}

	@Override
	protected Type getLongvarcharType(String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) throws UnknownTypeException {
		throw new UnknownTypeException("Unsuported JDBC type, code: -1. Oracle "
				+ typeName + " data type is not supported.");
	}

	@Override
	protected Type getOtherType(int dataType, String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) throws UnknownTypeException {
		Type type;
		// TODO define charset
		if (typeName.equalsIgnoreCase("NCHAR")) {
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.FALSE, "CHARSET");
			type.setSql99TypeName("CHARACTER");
		} else if (typeName.equalsIgnoreCase("NVARCHAR2")) {
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.TRUE, "CHARSET");
			type.setSql99TypeName("CHARACTER VARYING");
		} else if (typeName.equalsIgnoreCase("NCLOB")) {
			type = new SimpleTypeString(Integer.valueOf(columnSize),
					Boolean.TRUE, "CHARSET");
			type.setSql99TypeName("CHARACTER LARGE OBJECT");
		} else if (typeName.equalsIgnoreCase("ROWID")) {
			type = new SimpleTypeString(
					Integer.valueOf(columnSize), Boolean.TRUE);
			type.setSql99TypeName("CHARACTER VARYING");
		} else if (typeName.equalsIgnoreCase("UROWID")) {
			type = new SimpleTypeString(
					Integer.valueOf(columnSize), Boolean.TRUE);
			type.setSql99TypeName("CHARACTER VARYING");
		} else {
			type = super.getOtherType(dataType, typeName, columnSize,
					decimalDigits, numPrecRadix);
		}
		return type;
	}

	@Override
	protected Type getSpecificType(int dataType, String typeName,
			int columnSize, int decimalDigits, int numPrecRadix)
					throws UnknownTypeException {
		Type type;
		switch (dataType) {
		case OracleTypes.BINARY_DOUBLE:
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("BIT VARYING");
			break;
		case OracleTypes.BINARY_FLOAT:
			type = new SimpleTypeNumericApproximate(
					Integer.valueOf(columnSize));
			type.setSql99TypeName("BIT VARYING");
			break;
		// TODO add support to BFILEs
//		case OracleTypes.BFILE:
//			type = new SimpleTypeBinary();
//			break;
		case OracleTypes.TIMESTAMPTZ:
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
			type.setSql99TypeName("TIMESTAMP");
			break;
		case OracleTypes.TIMESTAMPLTZ:
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
			type.setSql99TypeName("TIMESTAMP");
			break;
		default:
			type = super.getSpecificType(dataType, typeName, columnSize,
					decimalDigits, numPrecRadix);
			break;
		}
		return type;
	}

	@Override
	protected String processActionTime(String string) {
		String[] parts = string.split("\\s+");
		String res = parts[0];
		if (res.equalsIgnoreCase("INSTEAD")) {
			res+= " OF";
		}
		return res;
	}
}
