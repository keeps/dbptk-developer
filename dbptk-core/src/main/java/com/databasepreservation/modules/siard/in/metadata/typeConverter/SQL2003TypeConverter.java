package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * Converts a SQL2003 normalized type to an internal type structure.
 *
 * @author Miguel Coutada
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQL2003TypeConverter implements TypeConverter {
  private static int getCLOBMinimum() {
    return 65535;
  }

  private static int getLength(String sqlStandardType) {
    int length;
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
    int precision;
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
    int scale;
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

  @Override
  public Type getType(String sqlStandardType, String originalType) throws ModuleException {
    sqlStandardType = sqlStandardType.toUpperCase();
    Type type;

    if (sqlStandardType.startsWith("INT")) {
      type = new SimpleTypeNumericExact(10, 0);
    } else if ("SMALLINT".equals(sqlStandardType)) {
      type = new SimpleTypeNumericExact(5, 0);
    } else if (sqlStandardType.startsWith("NUMERIC")) {
      type = new SimpleTypeNumericExact(getPrecision(sqlStandardType), getScale(sqlStandardType));
    } else if (sqlStandardType.startsWith("DEC")) {
      type = new SimpleTypeNumericExact(getPrecision(sqlStandardType), getScale(sqlStandardType));
    } else if ("FLOAT".equals(sqlStandardType)) {
      type = new SimpleTypeNumericApproximate(53);
    } else if (sqlStandardType.startsWith("FLOAT")) {
      type = new SimpleTypeNumericApproximate(getPrecision(sqlStandardType));
    } else if ("REAL".equals(sqlStandardType)) {
      type = new SimpleTypeNumericApproximate(24);
    } else if (sqlStandardType.startsWith("DOUBLE")) {
      type = new SimpleTypeNumericApproximate(53);
    } else if (sqlStandardType.startsWith("BINARY LARGE OBJECT") || sqlStandardType.startsWith("BLOB")) {
      type = new SimpleTypeBinary();
    } else if (sqlStandardType.startsWith("CHAR")) {
      if (isLargeObject(sqlStandardType)) {
        type = new SimpleTypeString(getCLOBMinimum(), true);
      } else {
        if (isLengthVariable(sqlStandardType)) {
          type = new SimpleTypeString(getLength(sqlStandardType), true);
        } else {
          type = new SimpleTypeString(getLength(sqlStandardType), false);
        }
      }
    } else if (sqlStandardType.startsWith("VARCHAR")) {
      type = new SimpleTypeString(getLength(sqlStandardType), true);
    } else if (sqlStandardType.startsWith("NATIONAL")) {
      if (isLargeObject(sqlStandardType) || sqlStandardType.startsWith("NCLOB")) {
        type = new SimpleTypeString(getCLOBMinimum(), true); // TODO: how to
                                                             // choose a
                                                             // charset?
      } else {
        if (isLengthVariable(sqlStandardType)) {
          type = new SimpleTypeString(getLength(sqlStandardType), true); // TODO:
                                                                         // how
                                                                         // to
                                                                         // choose
                                                                         // a
                                                                         // charset?
        } else {
          type = new SimpleTypeString(getLength(sqlStandardType), false); // TODO:
                                                                          // how
                                                                          // to
                                                                          // choose
                                                                          // a
                                                                          // charset?
        }
      }
    } else if ("BOOLEAN".equals(sqlStandardType)) {
      type = new SimpleTypeBoolean();
    } else if ("DATE".equals(sqlStandardType)) {
      type = new SimpleTypeDateTime(false, false);
    } else if ("TIMESTAMP WITH TIME ZONE".equals(sqlStandardType)) {
      type = new SimpleTypeDateTime(true, true);
    } else if ("TIMESTAMP".equals(sqlStandardType)) {
      type = new SimpleTypeDateTime(true, false);
    } else if ("TIME WITH TIME ZONE".equals(sqlStandardType)) {
      type = new SimpleTypeDateTime(true, true);
    } else if ("TIME".equals(sqlStandardType)) {
      type = new SimpleTypeDateTime(true, false);
    } else {
      // type = new SimpleTypeString(255, true);
      throw new ModuleException("unidentified sqlStandardType: " + sqlStandardType);
    }

    type.setSql2003TypeName(sqlStandardType);
    type.setOriginalTypeName(originalType);

    return type;
  }
}
