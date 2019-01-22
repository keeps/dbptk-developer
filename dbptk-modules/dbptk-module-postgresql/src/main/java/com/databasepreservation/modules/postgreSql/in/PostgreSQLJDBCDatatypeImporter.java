/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.postgreSql.in;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class PostgreSQLJDBCDatatypeImporter extends JDBCDatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLJDBCDatatypeImporter.class);

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    if ("varbit".equals(typeName)) {
      type.setSql99TypeName("BIT VARYING", 8 * columnSize);
      type.setSql2008TypeName("BINARY VARYING", 8 * columnSize);
    } else if ("bytea".equalsIgnoreCase(typeName)) {
      type.setSql99TypeName("BINARY LARGE OBJECT");
      type.setSql2008TypeName("BINARY LARGE OBJECT");
    } else {
      type.setSql99TypeName("BINARY LARGE OBJECT");
      type.setSql2008TypeName("BINARY LARGE OBJECT");
    }
    return type;
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if ("MONEY".equalsIgnoreCase(typeName) || "FLOAT8".equalsIgnoreCase(typeName)) {
      // TODO logger.warn("Setting Money column size to 53");
      columnSize = 53;
    }
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("DOUBLE PRECISION");
    type.setSql2008TypeName("DOUBLE PRECISION");
    return type;
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("TIMETZ".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeDateTime(true, true);
      type.setSql99TypeName("TIME WITH TIME ZONE");
      type.setSql2008TypeName("TIME WITH TIME ZONE");
    } else {
      type = new SimpleTypeDateTime(true, false);
      type.setSql99TypeName("TIME");
      type.setSql2008TypeName("TIME");
    }

    return type;
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("TIMESTAMPTZ".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeDateTime(true, true);
      type.setSql99TypeName("TIMESTAMP WITH TIME ZONE");
      type.setSql2008TypeName("TIMESTAMP WITH TIME ZONE");
    } else {
      type = new SimpleTypeDateTime(true, false);
      type.setSql99TypeName("TIMESTAMP");
      type.setSql2008TypeName("TIMESTAMP");
    }

    return type;
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    if ("text".equalsIgnoreCase(typeName)) {
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    } else {
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    }
    return type;
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    switch (dataType) {
      case 2009: // XML Data type
        type = new SimpleTypeString(columnSize, true);
        type.setSql99TypeName("CHARACTER LARGE OBJECT");
        type.setSql2008TypeName("CHARACTER LARGE OBJECT");
        break;
      default:
        type = super.getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }
    return type;
  }

  @Override
  protected Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema, int dataType,
    String typeName) {
    Type type = new SimpleTypeString(65535, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    if ("varbit".equals(typeName)) {
      return getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      return super.getOtherType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
    }
  }

  @Override
  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);

    // The maximum allowed precision when explicitly specified in the type
    // declaration is 1000, so if we find more than that it means that this type was
    // declared without precision nor scale
    if (columnSize > 1000) {
      type.setSql99TypeName("NUMERIC");
      type.setSql2008TypeName("NUMERIC");
    } else if (decimalDigits > 0) {
      type.setSql99TypeName("NUMERIC", columnSize, decimalDigits);
      type.setSql2008TypeName("NUMERIC", columnSize, decimalDigits);
    } else {
      type.setSql99TypeName("NUMERIC", columnSize);
      type.setSql2008TypeName("NUMERIC", columnSize);
    }
    return type;
  }

  // @Override
  // protected Type getArraySubTypeFromTypeName(String typeName, int columnSize,
  // int decimalDigits, int numPrecRadix,
  // int dataType) throws UnknownTypeException {
  // Type subtype = null;
  //
  // if (typeName != null && typeName.startsWith("_")) {
  // try {
  // subtype = getType(null, null, null, null, dataType, typeName.substring(1),
  // columnSize, decimalDigits, numPrecRadix);
  // } catch (NullPointerException | UnknownTypeException |
  // ClassNotFoundException | SQLException e) {
  // LOGGER
  // .debug(
  // "Exception trying to obtain array subtype for {typeName='{}', columnSize={},
  // decimalDigits={}, numPrecRadix={}, dataType={}}",
  // typeName, columnSize, decimalDigits, numPrecRadix, dataType, e);
  // }
  // }
  //
  // if(subtype == null){
  // subtype = super.getArraySubTypeFromTypeName(typeName, columnSize,
  // decimalDigits, numPrecRadix, dataType);
  // }
  // return subtype;
  // }
}
