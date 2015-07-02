/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.mySql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.util.StringUtils;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ForeignKey;
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

	private static final Set<String> MYSQL_TYPES = new HashSet<String>(
			Arrays.asList("BLOB", "MEDIUMBLOB", "LONGBLOB", "TIMESTAMP", "TINYBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT"));

	private String name = "MySQL";

	private String startQuote = "`";

	private String endQuote = "`";

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getStartQuote() {
		return startQuote;
	}

	@Override
	public String getEndQuote() {
		return endQuote;
	}

	@Override
	public String createTableSQL(TableStructure table)
			throws UnknownTypeException, ModuleException {
		return super.createTableSQL(table) + " ENGINE=INNODB";
	}

	@Override
	protected String createColumnSQL(ColumnStructure column,
			boolean isPrimaryKey, boolean isForeignKey)
			throws UnknownTypeException {
		return super.createColumnSQL(column, isPrimaryKey, isForeignKey)
				+ (column.getDescription() != null ? " COMMENT '"
						+ escapeComment(column.getDescription()) + "'" : "");
	}

	@Override
	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;

		logger.debug("Checking MySQL type " + type.getOriginalTypeName());
		if (MYSQL_TYPES.contains(type.getOriginalTypeName())) {
			// TODO verify if original database is also mysql
			ret = type.getOriginalTypeName();
			logger.warn("Using MySQL original type " + ret);
		} else if (type instanceof SimpleTypeString) {
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
			SimpleTypeNumericApproximate numericApprox = (SimpleTypeNumericApproximate) type;
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
			} else if (type.getSql99TypeName().equalsIgnoreCase("TIME")) {
				if (dateTime.getTimeZoneDefined()) {
					logger.warn("Timezone not supported on MySQL: "
							+ "defining type as 'time'");
				}
				ret = "time";
			} else {
				if (dateTime.getTimeZoneDefined()) {
					logger.warn("Timezone not supported on MySQL: "
							+ "defining type as 'datetime'");
				}
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
						ret = "binary("
								+ (((length / 8.0) % 1 == 0) ? (length / 8)
										: ((length / 8) + 1)) + ")";
					}
				} else if (type.getSql99TypeName().equalsIgnoreCase(
						"BIT VARYING")) {
					ret = "varbinary("
							+ (((length / 8.0) % 1 == 0) ? (length / 8)
									: ((length / 8) + 1)) + ")";
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

	/**
	 * SQL to create a foreign key (relation), altering the already created
	 * table
	 * 
	 * @param tableName
	 *            the name of the table
	 * @param fkey
	 *            the foreign key
	 * @param addConstraint
	 *            if an extra constraint info is need at creation of fkey
	 * @param plus
	 *            a plus factor that makes a foreign key name unique
	 * @return the SQL
	 * @throws ModuleException
	 */
	public String createForeignKeySQL(TableStructure table, ForeignKey fkey,
			boolean addConstraint, int plus) throws ModuleException {

		String foreignRefs = "";
		for (int i = 0; i < fkey.getReferences().size(); i++) {
			if (i > 0) {
				foreignRefs += ", ";
			}
			foreignRefs += escapeColumnName(fkey.getReferences().get(i)
					.getColumn());
			fkey.getReferences().get(i).getColumn();
		}

		String foreignReferenced = "";
		for (int i = 0; i < fkey.getReferences().size(); i++) {
			if (i > 0) {
				foreignReferenced += ", ";
			}
			foreignReferenced += escapeColumnName(fkey.getReferences().get(i)
					.getReferenced());
			fkey.getReferences().get(i).getReferenced();
		}

		String constraint = "";
		if (addConstraint) {
			constraint = " ADD CONSTRAINT `dbpres_"
					+ System.currentTimeMillis() + plus + "`";
		}
		String ret = "ALTER TABLE " + escapeTableName(table.getName())
				+ (addConstraint ? constraint : " ADD") + " FOREIGN KEY ("
				+ foreignRefs + ") REFERENCES "
				+ escapeTableName(fkey.getReferencedSchema()) + "."
				+ escapeTableName(fkey.getReferencedTable()) + " ("
				+ foreignReferenced + ")";
		return ret;
	}

	// MySQL does not support check constraints (returns an empty SQL query)
	@Override
	public String getCheckConstraintsSQL(String schemaName, String tableName) {
		return "";
	}

	@Override
	public String getTriggersSQL(String schemaName, String tableName) {
		return "SELECT " + "trigger_name AS TRIGGER_NAME, "
				+ "action_timing AS ACTION_TIME, "
				+ "event_manipulation AS TRIGGER_EVENT, "
				+ "action_statement AS TRIGGERED_ACTION "
				+ "FROM information_schema.triggers "
				+ "WHERE trigger_schema='" + schemaName + "' "
				+ "AND event_object_table='" + tableName + "'";
	}

	@Override
	public String getUsersSQL(String dbName) {
		return "SELECT * FROM `mysql`.`user`";
	}

	protected String escapeComment(String description) {
		return description.replaceAll("'", "''");
	}

	@Override
	public String getDatabases(String database) {
		return "SHOW DATABASES LIKE '" + database + "\\_%';";
	}

	@Override
	public String dropDatabase(String database) {
		return "DROP DATABASE IF EXISTS " + database;
	}

	@Override
	protected String escapePrimaryKeyName(String pkey_name) {
		if (pkey_name.equals("PRIMARY")) {
			logger.warn("Cannot set primary key name to reserved name PRIMARY, renaming it");
			pkey_name += "_pkey";
		}

		return super.escapePrimaryKeyName(pkey_name);
	}

}
