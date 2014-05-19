/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.mySql;

import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class MySQLHelper extends SQLHelper {

	private final Logger logger = Logger.getLogger(MySQLHelper.class);
	
	private String name = "MySQL";
	
	private String startQuote = "`";
	
	private String endQuote = "`";
	
	public String getName() {
		return name;
	}
	
	public String getStartQuote() {
		return startQuote;
	}

	public String getEndQuote() {
		return endQuote;
	}

	public String createTableSQL(TableStructure table)
			throws UnknownTypeException, ModuleException {
		logger.debug("mysql");
		return super.createTableSQL(table) + " ENGINE=INNODB";
	}

	protected String createColumnSQL(ColumnStructure column,
			boolean isPrimaryKey, boolean isForeignKey)
			throws UnknownTypeException {
		return super.createColumnSQL(column, isPrimaryKey, isForeignKey)
				+ (column.getDescription() != null ? " COMMENT '"
						+ escapeComment(column.getDescription()) + "'" : "");
	}

	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;
		if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (isPkey) {
				int length = string.getLength().intValue();
				if (length >= 65535) {
					logger.warn("Resizing column length to 333 "
							+ "so it can be a primary key");
					length = 333;
				}
				ret = "varchar(" + length + ")";
			} else if (string.isLengthVariable()) {
				if (string.getLength().intValue() >= 65535) {
					ret = "longtext";
				} else {
					ret = "varchar(" + string.getLength() + ")";
				}
			} else {
				if (string.getLength().intValue() >= 255) {
					ret = "text";
				} else {
					ret = "char(" + string.getLength() + ")";
				}
			}
		} else if (type instanceof SimpleTypeNumericApproximate) {
			SimpleTypeNumericApproximate numericApprox = 
					(SimpleTypeNumericApproximate) type;
			if (type.getSql99TypeName().equalsIgnoreCase("REAL")) {
				ret = "float(12)";
			} else if (StringUtils.startsWithIgnoreCase(
					type.getSql99TypeName(), "DOUBLE")) {
				ret = "double";
			} else {
				logger.debug("float precision: " + numericApprox.getPrecision());
				ret = "float(" + numericApprox.getPrecision() + ")";
			}
		} else if (type instanceof SimpleTypeBoolean) {
			ret = "bit(1)";
		} else if (type instanceof SimpleTypeDateTime) {
			SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
			if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
				ret = "date";
			} else if (dateTime.getTimeZoneDefined()) {
				throw new UnknownTypeException(
						"Time zone not supported in MySQL");
			} else if (type.getSql99TypeName().equalsIgnoreCase("TIME")) { 
				ret = "time";
			} else {
				ret = "datetime";
			}
		} else if (type instanceof SimpleTypeBinary) {
			SimpleTypeBinary binary = (SimpleTypeBinary) type;
			Integer length = binary.getLength();
			if (length != null) {
				if (type.getSql99TypeName().equalsIgnoreCase("BIT")) {
					if (type.getOriginalTypeName().equalsIgnoreCase("BIT")) {
						ret = "bit(" + length + ")";
					} else {
						ret = "binary(" + (((length / 8.0) % 1 == 0) ? 
								(length / 8) : ((length / 8) + 1)) + ")";
					}
				} else if (type.
						getSql99TypeName().equalsIgnoreCase("BIT VARYING")) {
					ret = "varbinary(" + (((length / 8.0) % 1 == 0) ? 
							(length / 8) : ((length / 8) + 1)) + ")";
				} else {
					ret = "longblob";
				}
			} else {
				ret = "longblob";
			}
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}
	
	// MySQL does not support check constraints
	public String getCheckConstraintsSQL(String schemaName, String tableName) {
		return "";
		// return super.getCheckConstraintsSQL(schemaName, tableName);
	}
	
	public String getTriggersSQL(String schemaName, String tableName) {
		return "SELECT "
				+ "trigger_name AS TRIGGER_NAME, "
				+ "action_timing AS ACTION_TIME, "
				+ "event_manipulation AS TRIGGER_EVENT, "
				+ "action_statement AS TRIGGERED_ACTION "
				+ "FROM information_schema.triggers "
				+ "WHERE trigger_schema='" + schemaName + "' " 
				+ "AND event_object_table='" + tableName + "'";
	}
	
	public String getUsersSQL(String dbName) {
		return "SELECT * FROM `mysql`.`user`";
	}
	
	protected String escapeComment(String description) {
		return description.replaceAll("'", "''");
	}
}
