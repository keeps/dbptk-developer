/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle.in;

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

import oracle.jdbc.OracleTypes;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class Oracle12cJDBCDatatypeImporter extends JDBCDatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(Oracle12cJDBCDatatypeImporter.class.getName());
  private static final int CHAR_MIN_COLUMN_SIZE_VALUE = 1;
  private static final int NUMERIC_MAX_COLUMN_SIZE_VALUE = 38;
  private static final int NUMERIC_MAX_COLUMN_DECIMAL_DIGITS_VALUE = 127;

  @Override
  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);

    if (columnSize == 0) {
      LOGGER.debug("Column data length is 0 replacing the length to the max data length {}",
        NUMERIC_MAX_COLUMN_SIZE_VALUE);
      type.setSql99TypeName("NUMERIC", NUMERIC_MAX_COLUMN_SIZE_VALUE, NUMERIC_MAX_COLUMN_DECIMAL_DIGITS_VALUE);
      type.setSql2008TypeName("NUMERIC", NUMERIC_MAX_COLUMN_SIZE_VALUE, NUMERIC_MAX_COLUMN_DECIMAL_DIGITS_VALUE);
    } else {
      if (decimalDigits > 0) {
        type.setSql99TypeName("NUMERIC", columnSize, decimalDigits);
        type.setSql2008TypeName("NUMERIC", columnSize, decimalDigits);
      } else {
        type.setSql99TypeName("NUMERIC", columnSize);
        type.setSql2008TypeName("NUMERIC", columnSize);
      }
    }
    return type;
  }

  @Override
  protected Type getLongVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return getClobType(typeName, columnSize, decimalDigits, numPrecRadix);
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.equalsIgnoreCase("RAW") || typeName.equalsIgnoreCase("LONG RAW")) {
      return getLongVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      return super.getVarbinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
  }

  @Override
  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    // special case when NUMBER is defined without specifying precision nor
    // scale
    if (columnSize == 0 && decimalDigits == -127 && numPrecRadix == 10) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("DOUBLE PRECISION");
      type.setSql2008TypeName("DOUBLE PRECISION");
    }
    // for all other cases NUMBER is a DECIMAL
    else {
      type = new SimpleTypeNumericExact(columnSize, decimalDigits);
      if (decimalDigits > 0) {
        type.setSql99TypeName("DECIMAL", columnSize, decimalDigits);
        type.setSql2008TypeName("DECIMAL", columnSize, decimalDigits);
      } else {
        type.setSql99TypeName("DECIMAL", columnSize);
        type.setSql2008TypeName("DECIMAL", columnSize);
      }
    }
    return type;
  }

  @Override
  protected Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    if (columnSize == 0) {
      LOGGER.debug("Column data length is 0 replacing the length to the minimum required {}",
        CHAR_MIN_COLUMN_SIZE_VALUE);
      type.setSql99TypeName("CHARACTER", CHAR_MIN_COLUMN_SIZE_VALUE);
      type.setSql2008TypeName("CHARACTER", CHAR_MIN_COLUMN_SIZE_VALUE);
    } else {
      type.setSql99TypeName("CHARACTER", columnSize);
      type.setSql2008TypeName("CHARACTER", columnSize);
    }
    type.setOriginalTypeName("CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("VARCHAR2".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(columnSize, true);
      if (columnSize == 0) {
        LOGGER.debug("Column data length is 0 replacing the length to the minimum required {}",
          CHAR_MIN_COLUMN_SIZE_VALUE);
        type.setSql99TypeName("CHARACTER VARYING", CHAR_MIN_COLUMN_SIZE_VALUE);
        type.setSql2008TypeName("CHARACTER VARYING", CHAR_MIN_COLUMN_SIZE_VALUE);
      } else {
        type.setSql99TypeName("CHARACTER VARYING", columnSize);
        type.setSql2008TypeName("CHARACTER VARYING", columnSize);
      }
      type.setOriginalTypeName(typeName, columnSize);
    } else {
      type = super.getVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
    return type;
  }

  @Override
  protected Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    // TODO: add charset
    Type type;
    if ("NVARCHAR2".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(2000, true);
      type.setSql99TypeName("NATIONAL CHARACTER VARYING", 2000);
      type.setSql2008TypeName("NATIONAL CHARACTER VARYING", 2000);
    } else {
      type = super.getNationalVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
    return type;
  }

  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    // TODO define charset
    if ("NCHAR".equalsIgnoreCase(typeName)) {
      type = getNationalCharType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else if ("NVARCHAR2".equalsIgnoreCase(typeName)) {
      type = getNationalVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else if ("VARCHAR2".equalsIgnoreCase(typeName)) {
      type = getVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else if ("NCLOB".equalsIgnoreCase(typeName)) {
      type = getLongNationalVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else if ("ROWID".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true);
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    } else if ("UROWID".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true);
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    } else {
      // try to get everything else as string
      type = new SimpleTypeString(65535, true);
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2008TypeName("CHARACTER LARGE OBJECT");
      // type = super.getOtherType(dataType, typeName, columnSize,
      // decimalDigits, numPrecRadix);
    }
    return type;
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    switch (dataType) {
      case OracleTypes.BINARY_DOUBLE:
        type = new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
        type.setSql99TypeName("BIT VARYING", columnSize); // todo: not sure if
        // columnSize is the
        // correct value here
        type.setSql2008TypeName("BIT VARYING", columnSize); // todo: not sure if
        // columnSize is the
        // correct value
        // here
        break;
      case OracleTypes.BINARY_FLOAT:
        type = new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
        type.setSql99TypeName("BIT VARYING", columnSize); // todo: not sure if
        // columnSize is the
        // correct value here
        type.setSql2008TypeName("BIT VARYING", columnSize); // todo: not sure if
        // columnSize is the
        // correct value
        // here
        break;
      // TODO add support to BFILEs
      case OracleTypes.BFILE:
        type = new SimpleTypeBinary();
        type.setSql99TypeName("BINARY LARGE OBJECT");
        type.setSql2008TypeName("BINARY LARGE OBJECT");
        break;
      case OracleTypes.TIMESTAMPTZ:
        type = new SimpleTypeDateTime(true, true);
        type.setSql99TypeName("TIMESTAMP");
        type.setSql2008TypeName("TIMESTAMP");
        break;
      case OracleTypes.TIMESTAMPLTZ:
        type = new SimpleTypeDateTime(true, true);
        type.setSql99TypeName("TIMESTAMP");
        type.setSql2008TypeName("TIMESTAMP");
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
    if ("SDO_GEOMETRY".equalsIgnoreCase(typeName)) {
      return getLongVarcharType(typeName, 0, 0, 0);
    } else {
      return super.getComposedTypeStructure(database, currentSchema, dataType, typeName);
    }
  }
}
