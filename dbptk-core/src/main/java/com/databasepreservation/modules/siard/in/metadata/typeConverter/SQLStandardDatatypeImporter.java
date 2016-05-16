package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatatypeImporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class SQLStandardDatatypeImporter extends DatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLStandardDatatypeImporter.class.getName());

  private static final int DEFAULT_COLUMN_SIZE = 0;
  private static final int DEFAULT_DECIMAL_DIGITS = 0;
  private static final int DEFAULT_NUM_PREC_RADIX = 10;

  private static final int MINIMUM_FLOAT_PRECISION = 53;
  private static final int MINIMUM_DOUBLE_PRECISION = 53;
  private static final int MINIMUM_REAL_PRECISION = 24;

  private static final int MINIMUM_TINYINT_SIZE = 5;
  private static final int MINIMUM_SMALLINT_SIZE = 5;
  private static final int MINIMUM_INT_SIZE = 10;
  private static final int MINIMUM_BIGINT_SIZE = 20;

  private static final int MINIMUM_CLOB_SIZE = 65535;

  /**
   * getCheckedType method, simplified to use with SIARD import modules
   */
  public Type getCheckedType(String databaseName, String schemaName, String tableName, String columnName,
    String sqlStandardType, String originalType) {
    // dummy objects with bare essentials to be compatible with the parent class
    DatabaseStructure database = new DatabaseStructure();
    database.setName(databaseName);
    SchemaStructure schema = new SchemaStructure();
    schema.setName(schemaName);

    int columnSize = DEFAULT_COLUMN_SIZE;
    int decimalDigits = DEFAULT_DECIMAL_DIGITS;
    int numPrecRadix = DEFAULT_NUM_PREC_RADIX;

    int indexOfOpen = sqlStandardType.indexOf('(');
    int indexOfComma = sqlStandardType.indexOf(',', indexOfOpen);
    int indexOfClose = sqlStandardType.indexOf(')', indexOfComma);

    // attempt to parse SQL standard type
    // TODO: support parameters other than those matching [+-]?[0-9]+
    try {
      if (indexOfOpen >= 0 && indexOfComma >= 0 && indexOfClose >= 0) {
        // format like NAME(PARAM1,PARAM2)
        columnSize = Integer.parseInt(sqlStandardType.substring(indexOfOpen + 1, indexOfComma).trim());
        decimalDigits = Integer.parseInt(sqlStandardType.substring(indexOfComma + 1, indexOfClose).trim());
      } else if (indexOfOpen >= 0 && indexOfClose >= 0) {
        // format like NAME(PARAM1)
        columnSize = Integer.parseInt(sqlStandardType.substring(indexOfOpen + 1, indexOfClose).trim());
      }
    } catch (NumberFormatException e) {
      columnSize = DEFAULT_COLUMN_SIZE;
      decimalDigits = DEFAULT_DECIMAL_DIGITS;
    }

    Type type = getCheckedType(database, schema, tableName, columnName, 0, sqlStandardType, columnSize, decimalDigits,
      numPrecRadix);
    type.setOriginalTypeName(originalType);
    return type;
  }

  @Override
  protected Type getArray(String typeName, int columnSize, int decimalDigits, int numPrecRadix, int dataType)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("NATIONAL CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("NATIONAL CHARACTER VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getTinyintType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_TINYINT_SIZE), 0);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getSmallIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_SMALLINT_SIZE), 0);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getLongNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("NATIONAL CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("NATIONAL CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getIntegerType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_INT_SIZE), 0);
    type.setSql99TypeName("INTEGER");
    type.setSql2008TypeName("INTEGER");
    return type;
  }

  @Override
  protected Type getClobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(Math.max(columnSize, MINIMUM_CLOB_SIZE), true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getNationalCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    // TODO add charset
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("NATIONAL CHARACTER", columnSize);
    type.setSql2008TypeName("NATIONAL CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("CHARACTER", columnSize);
    type.setSql2008TypeName("CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getBooleanType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeBoolean type = new SimpleTypeBoolean();
    type.setSql99TypeName("BOOLEAN");
    type.setSql2008TypeName("BOOLEAN");
    return type;
  }

  @Override
  protected Type getBlobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeBinary type;
    if (columnSize > 0) {
      type = new SimpleTypeBinary(columnSize);
    } else {
      type = new SimpleTypeBinary();
    }
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getBitType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if (columnSize > 1) {
      type = new SimpleTypeBinary(columnSize);
      type.setSql99TypeName("BIT", columnSize);
      type.setSql2008TypeName("BIT", columnSize);
    } else {
      type = new SimpleTypeBoolean();
      type.setSql99TypeName("BOOLEAN");
      type.setSql2008TypeName("BOOLEAN");
    }
    return type;
  }

  @Override
  protected Type getBigIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(Math.max(MINIMUM_BIGINT_SIZE, columnSize), 0);
  }

  @Override
  protected Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema, int dataType,
    String typeName) {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT VARYING", columnSize);
    type.setSql2008TypeName("BIT VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_REAL_PRECISION));
  }

  @Override
  protected Type getArraySubTypeFromTypeName(String typeName, int columnSize, int decimalDigits, int numPrecRadix,
    int dataType) throws UnknownTypeException {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeBinary(columnSize);
  }

  @Override
  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeDateTime(false, false);
  }

  @Override
  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_DOUBLE_PRECISION));
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_FLOAT_PRECISION));
  }

  @Override
  protected Type getLongvarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    if (columnSize != 0) {
      type = new SimpleTypeString(columnSize, true);
    } else {
      type = new SimpleTypeString(Integer.MAX_VALUE, true);
    }

    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    } else {
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    } else {
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getFallbackType(String originalTypeName) {
    Type type = super.getFallbackType(originalTypeName);
    // this will be filled later, before returning from getCheckedType
    type.setOriginalTypeName(null);
    return type;
  }
}
