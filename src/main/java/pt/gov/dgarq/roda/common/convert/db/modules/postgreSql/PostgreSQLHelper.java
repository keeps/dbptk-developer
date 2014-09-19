/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.postgreSql;

import org.apache.commons.lang3.StringUtils;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class PostgreSQLHelper extends SQLHelper {
	
	private String startQuote = "\"";
	
	private String endQuote = "\"";
	
	public String getStartQuote() {
		return startQuote;
	}

	public String getEndQuote() {
		return endQuote;
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

	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;
		if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (string.getLength().intValue() >= 65535) {
				ret = "text";
			} else if (string.isLengthVariable()) {
				ret = "varchar(" + string.getLength() + ")";
			} else {
				ret = "char(" + string.getLength() + ")";
			}
		} else if (type instanceof SimpleTypeNumericApproximate) {
			SimpleTypeNumericApproximate numericApproximate = 
					(SimpleTypeNumericApproximate) type;
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
					if (dateTime.getTimeZoneDefined()) {
						ret = "timestamp with time zone";
					} else {
						ret = "timestamp without time zone";
					}
				} else {
					if (dateTime.getTimeDefined()) {
						ret = "time with time zone";
					} else {
						ret = "time without time zone";
					}
				}
			}
		} else if (type instanceof SimpleTypeBinary) {
			ret = "bytea";
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}

	
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
				+ "WHERE t.relname='" + tableName + "' "
				+ "AND n.nspname='" + schemaName + "'"; 
	}
	
	public String getTriggersSQL(String schemaName, String tableName) {
		return "SELECT "
				+ "trigger_name AS TRIGGER_NAME, action_timing AS ACTION_TIME, "
				+ "event_manipulation AS TRIGGER_EVENT, "
				+ "action_statement AS TRIGGERED_ACTION "
				+ "FROM information_schema.triggers "
				+ "WHERE trigger_schema='" + schemaName 
				+ "' AND event_object_table='" + tableName + "'";
	}
	
	public String getUsersSQL(String dbName) {
		return "SELECT usename AS USER_NAME FROM pg_catalog.pg_user";
	}
	
	public String getRolesSQL() {
		return "SELECT rolname AS ROLE_NAME FROM pg_roles";
	}
}
