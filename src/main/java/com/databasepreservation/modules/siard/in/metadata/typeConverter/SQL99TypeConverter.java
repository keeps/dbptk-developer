package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.*;

/**
 * Converts a SQL99 normalized type to an internal type structure.
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQL99TypeConverter implements TypeConverter {
	@Override
	public Type getType(String sqlStandardType, String originalType) throws ModuleException {
		sqlStandardType = sqlStandardType.toUpperCase();
		//logger.debug("sqlType: " + sql99Type);
		Type type = null;

		if (sqlStandardType.startsWith("INT")) {
			type = new SimpleTypeNumericExact(10, 0);
		} else if (sqlStandardType.equals("SMALLINT")) {
			type = new SimpleTypeNumericExact(5, 0);
		} else if (sqlStandardType.startsWith("NUMERIC")) {
			type = new SimpleTypeNumericExact(getPrecision(sqlStandardType),
					getScale(sqlStandardType));
		} else if (sqlStandardType.startsWith("DEC")) {
			type = new SimpleTypeNumericExact(getPrecision(sqlStandardType),
					getScale(sqlStandardType));
		} else if (sqlStandardType.equals("FLOAT")) {
			type = new SimpleTypeNumericApproximate(53);
		} else if (sqlStandardType.startsWith("FLOAT")) {
			type = new SimpleTypeNumericApproximate(getPrecision(sqlStandardType));
		} else if (sqlStandardType.equals("REAL")) {
			type = new SimpleTypeNumericApproximate(24);
		} else if (sqlStandardType.startsWith("DOUBLE")) {
			type = new SimpleTypeNumericApproximate(53);
		} else if (sqlStandardType.equals("BIT")) {
			type = new SimpleTypeBoolean();
		} else if (sqlStandardType.startsWith("BIT VARYING")) {
			type = new SimpleTypeBinary(getLength(sqlStandardType));
		} else if (sqlStandardType.startsWith("BIT")) {
			if (getLength(sqlStandardType) == 1) {
				type = new SimpleTypeBoolean();
			} else {
				type = new SimpleTypeBinary(getLength(sqlStandardType));
			}
		} else if (sqlStandardType.startsWith("BINARY LARGE OBJECT")
				|| sqlStandardType.startsWith("BLOB")) {
			type = new SimpleTypeBinary();
		} else if (sqlStandardType.startsWith("CHAR")) {
			if (isLargeObject(sqlStandardType)) {
				type = new SimpleTypeString(getCLOBMinimum(), Boolean.TRUE);
			} else {
				if (isLengthVariable(sqlStandardType)) {
					type = new SimpleTypeString(getLength(sqlStandardType),
							Boolean.TRUE);
				} else {
					type = new SimpleTypeString(getLength(sqlStandardType),
							Boolean.FALSE);
				}
			}
		} else if (sqlStandardType.startsWith("VARCHAR")) {
			type = new SimpleTypeString(getLength(sqlStandardType), Boolean.TRUE);
		} else if (sqlStandardType.startsWith("NATIONAL")) {
			if (isLargeObject(sqlStandardType) || sqlStandardType.startsWith("NCLOB")) {
				type = new SimpleTypeString(getCLOBMinimum(), Boolean.TRUE); // TODO: how to choose a charset?
			} else {
				if (isLengthVariable(sqlStandardType)) {
					type = new SimpleTypeString(getLength(sqlStandardType), Boolean.TRUE); // TODO: how to choose a charset?
				} else {
					type = new SimpleTypeString(getLength(sqlStandardType), Boolean.FALSE); // TODO: how to choose a charset?
				}
			}
		} else if (sqlStandardType.equals("BOOLEAN")) {
			type = new SimpleTypeBoolean();
		} else if (sqlStandardType.equals("DATE")) {
			type = new SimpleTypeDateTime(Boolean.FALSE, Boolean.FALSE);
		}  else if (sqlStandardType.equals("TIMESTAMP WITH TIME ZONE")) {
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
		}  else if (sqlStandardType.equals("TIMESTAMP")) {
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		} else if (sqlStandardType.equals("TIME WITH TIME ZONE")) {
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.TRUE);
		} else if (sqlStandardType.equals("TIME")) {
			type = new SimpleTypeDateTime(Boolean.TRUE, Boolean.FALSE);
		} else {
			type = new SimpleTypeString(255, Boolean.TRUE);
		}

		type.setSql99TypeName(sqlStandardType);
		type.setOriginalTypeName(originalType);

		return type;
	}

	private static int getCLOBMinimum() {
		return 65535;
	}

	private static int getLength(String sqlStandardType) {
		int length = -1;
		int start = sqlStandardType.indexOf("(");
		int end = sqlStandardType.indexOf(")");

		if (start < 0) {
			length = 1;
		} else {
			length = Integer.parseInt(sqlStandardType.substring(start + 1, end));
		}
		return length;
	}

	private static int getPrecision(String sqlStandardType) {
		int precision = -1;
		int start = sqlStandardType.indexOf("(");
		int end = sqlStandardType.indexOf(",");

		if (end < 0) {
			end = sqlStandardType.indexOf(")");
		}

		if (start < 0) {
			precision = 1;
		} else {
			precision = Integer.parseInt(sqlStandardType.substring(start + 1, end));
		}
		return precision;
	}

	private static int getScale(String sqlStandardType) {
		int scale = -1;
		int start = sqlStandardType.indexOf(",");
		int end = sqlStandardType.indexOf(")");
		if (start < 0) {
			scale = 0;
		} else {
			scale = Integer.parseInt(sqlStandardType.substring(start + 1, end));
		}
		return scale;
	}

	private static boolean isLengthVariable(String sqlStandardType) {
		return sqlStandardType.contains("VARYING");
	}

	private static boolean isLargeObject(String sqlStandardType) {
		return (sqlStandardType.contains("LARGE OBJECT") || sqlStandardType.contains("LOB"));
	}
}
