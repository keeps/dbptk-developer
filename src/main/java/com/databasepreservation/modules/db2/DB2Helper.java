/**
 * 
 */
package com.databasepreservation.modules.db2;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.SQLHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * @author Miguel Coutada
 * 
 */
public class DB2Helper extends SQLHelper {

	private final Logger logger = Logger.getLogger(DB2Helper.class);

	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;
		if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (isPkey) {
				int length = string.getLength().intValue();
				if (length >= 65535) {
					logger.warn("Resizing column length to 333 so "
							+ "it can be a primary key");
					length = 333;
				}
				ret = "varchar(" + length + ")";
			} else if (StringUtils.endsWithIgnoreCase(string.getSql99TypeName(),
					"CHARACTER LARGE OBJECT")) {
				if (string.getOriginalTypeName().equalsIgnoreCase("xml")) {
					ret = "xml";
				} else {
					ret = "clob";
				}
			} else { 
				if (StringUtils.endsWithIgnoreCase(string.getSql99TypeName(),
						"CHARACTER VARYING")) {
					ret = "varchar(" + string.getLength() + ")";
				} else {
					ret = "char(" + string.getLength() + ")";
				}
			}
		} else if (type instanceof SimpleTypeBoolean) {
			ret = "char";
		} else if (type instanceof SimpleTypeBinary) {
			ret = "blob";
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}

	protected String escapeColumnName(String column) {
		return column;
	}

	@Override
	public String getTriggersSQL(String schemaName, String tableName) {
		return "SELECT "
				+ "trigname AS TRIGGER_NAME, trigtime AS ACTION_TIME, "
				+ "trigevent AS TRIGGER_EVENT, text AS TRIGGERED_ACTION, "
				+ "remarks AS REMARKS "
				+ "FROM syscat.triggers "
				+ "WHERE tabname='" + tableName 
				+ "' AND tabschema='" + schemaName + "'";
	}

	@Override
	public String getCheckConstraintsSQL(String schemaName, String tableName) {
		return "SELECT constname AS CHECK_NAME, text AS CHECK_CONDITION "
				+ "FROM syscat.checks "
				+ "WHERE tabschema='" + schemaName 
				+ "' AND tabname='" + tableName + "' AND type='C'";
	}

	@Override
	public String getUsersSQL(String dbName) {
		return "SELECT grantee AS USER_NAME FROM syscat.dbauth";
	}

	@Override
	public String getRolesSQL() {
		return "SELECT rolename AS ROLE_NAME, grantee AS ADMIN "
				+ "FROM syscat.roleauth"; 
	}
	
	
}
