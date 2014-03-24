/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.postgreSql;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
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
			if (string.getLength().intValue() > 10485760) {
				ret = "text";
			} else if (string.isLengthVariable()) {
				ret = "varchar(" + string.getLength() + ")";
			} else {
				ret = "char(" + string.getLength() + ")";
			}
		} else if (type instanceof SimpleTypeDateTime) {
			SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
			if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
				ret = "date";
			} else if (dateTime.getTimeZoneDefined()) {
				ret = "timestamp with time zone";
			} else {
				ret = "timestamp without time zone";
			}

		} else if (type instanceof SimpleTypeBinary) {
			ret = "bytea";
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}
}
