/**
 * 
 */
package com.database_preservation.modules.postgreSql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.database_preservation.model.exception.ModuleException;
import com.database_preservation.model.exception.UnknownTypeException;
import com.database_preservation.model.structure.type.SimpleTypeBinary;
import com.database_preservation.model.structure.type.SimpleTypeDateTime;
import com.database_preservation.model.structure.type.SimpleTypeNumericApproximate;
import com.database_preservation.model.structure.type.SimpleTypeString;
import com.database_preservation.model.structure.type.Type;
import com.database_preservation.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class PostgreSQLHelper extends SQLHelper {

	private static final Set<String> POSTGRESQL_TYPES = new HashSet<String>(
			Arrays.asList("char", "int8", "varchar", "bigserial", "name",
					"numeric"));

	private final Logger logger = Logger.getLogger(getClass());

	private String startQuote = "\"";

	private String endQuote = "\"";

	@Override
	public String getStartQuote() {
		return startQuote;
	}

	@Override
	public String getEndQuote() {
		return endQuote;
	}

	@Override
	public String escapeSchemaName(String schema) {
		schema = schema.toLowerCase();
		return getStartQuote() + schema + getEndQuote();
	}

	@Override
	public String escapeTableName(String table) {
		table = table.toLowerCase();
		return getStartQuote() + table + getEndQuote();
	}

	/**
	 * Grant table read permissions to table schema
	 * 
	 * @param tableName
	 *            the table id
	 * @return the SQL
	 * @throws ModuleException
	 */
	public String grantPermissionsSQL(String tableId) throws ModuleException {
		String[] parts = splitTableId(tableId);
		String schema = parts[0];
		String table = parts[1];
		return "GRANT SELECT ON " + escapeSchemaName(schema) + "."
				+ escapeTableName(table) + " TO PUBLIC";
	}

	@Override
	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;

		logger.debug("Checking PSQL type " + type.getOriginalTypeName());
		if (POSTGRESQL_TYPES.contains(type.getOriginalTypeName())) {
			// TODO verify if original database is also postgresql
			ret = type.getOriginalTypeName();
			if (ret.equals("char")) {
				ret = "\"char\"";
			}
			logger.warn("Using PostgreSQL original type " + ret);
		} else if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (string.getLength().intValue() >= 65535) {
				ret = "text";
			} else if (string.isLengthVariable()) {
				ret = "varchar(" + string.getLength() + ")";
			} else {
				ret = "char(" + string.getLength() + ")";
			}
		} else if (type instanceof SimpleTypeNumericApproximate) {
			SimpleTypeNumericApproximate numericApproximate = (SimpleTypeNumericApproximate) type;
			if (type.getSql99TypeName().equalsIgnoreCase("REAL")) {
				ret = "real";
			} else if (StringUtils.startsWithIgnoreCase(
					type.getSql99TypeName(), "DOUBLE")) {
				ret = "double precision";
			} else {
				ret = "float(" + numericApproximate.getPrecision() + ")";
			}
		} else if (type instanceof SimpleTypeDateTime) {
			SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
			if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
				ret = "date";
			} else {
				if (type.getSql99TypeName().equalsIgnoreCase("TIMESTAMP")) {
					ret = "timestamp without time zone";
				} else if (type.getSql99TypeName().equalsIgnoreCase(
						"TIMESTAMP WITH TIME ZONE")) {
					ret = "timestamp with time zone";
				} else if (type.getSql99TypeName().equalsIgnoreCase("TIME")) {
					ret = "time without time zone";
				} else if (type.getSql99TypeName().equalsIgnoreCase(
						"TIME WITH TIME ZONE")) {
					ret = "time with time zone";
				} else {
					throw new UnknownTypeException(type.toString());
				}
			}
		} else if (type instanceof SimpleTypeBinary) {
			ret = "bytea";
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}

	@Override
	public String getCheckConstraintsSQL(String schemaName, String tableName) {
		return "SELECT tc.constraint_name AS CHECK_NAME "
				+ "FROM information_schema.table_constraints tc "
				+ "WHERE table_name='" + tableName + "' AND table_schema='"
				+ schemaName + "' AND constraint_type = 'CHECK'";
	}

	public String getCheckConstraintsSQL2(String schemaName, String tableName) {
		return "SELECT conname FROM pg_catalog.pg_constraint c "
				+ "LEFT JOIN pg_class t ON c.conrelid = t.oid "
				+ "LEFT JOIN pg_namespace n ON t.relnamespace = n.oid"
				+ "WHERE t.relname='" + tableName + "' " + "AND n.nspname='"
				+ schemaName + "'";
	}

	@Override
	public String getTriggersSQL(String schemaName, String tableName) {
		return "SELECT "
				+ "trigger_name AS TRIGGER_NAME, action_timing AS ACTION_TIME, "
				+ "event_manipulation AS TRIGGER_EVENT, "
				+ "action_statement AS TRIGGERED_ACTION "
				+ "FROM information_schema.triggers "
				+ "WHERE trigger_schema='" + schemaName
				+ "' AND event_object_table='" + tableName + "'";
	}

	@Override
	public String getUsersSQL(String dbName) {
		return "SELECT usename AS USER_NAME FROM pg_catalog.pg_user";
	}

	@Override
	public String getRolesSQL() {
		return "SELECT rolname AS ROLE_NAME FROM pg_roles";
	}

	@Override
	public String getDatabases(String database) {
		return "SELECT datname FROM pg_database WHERE datistemplate = false;";
	}

	@Override
	public String dropDatabase(String database) {
		return "DROP DATABASE IF EXISTS " + database;
	}
}
