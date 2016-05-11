package com.databasepreservation.modules.jdbc.in;

import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatatypeImporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.model.structure.type.UnsupportedDataType;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class JDBCDatatypeImporter extends DatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(JDBCDatatypeImporter.class);

  @Override
  protected Type getType(DatabaseStructure database, SchemaStructure currentSchema, String tableName,
    String columnName, int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException, SQLException, ClassNotFoundException {
    Type type;
    switch (dataType) {
      case Types.BIGINT:
        type = getBigIntType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.BINARY:
        type = getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.BIT:
        type = getBitType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.BLOB:
        type = getBlobType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.BOOLEAN:
        type = getBooleanType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.CHAR:
        type = getCharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.NCHAR:
        type = getNationalCharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.CLOB:
        type = getClobType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.DATE:
        type = getDateType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.DECIMAL:
        type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
        // TODO: make sure this check is not needed and remove it
        if (StringUtils.isBlank(type.getOriginalTypeName())) {
          type.setOriginalTypeName(typeName, columnSize, decimalDigits);
        }
        break;
      case Types.DOUBLE:
        type = getDoubleType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.FLOAT:
        type = getFloatType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.INTEGER:
        type = getIntegerType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.LONGVARBINARY:
        type = getLongvarbinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.LONGVARCHAR:
        type = getLongvarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.LONGNVARCHAR:
        type = getLongNationalVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.NUMERIC:
        type = getNumericType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.REAL:
        type = getRealType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.SMALLINT:
        type = getSmallIntType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.TIME:
        type = getTimeType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.TIMESTAMP:
        type = getTimestampType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.TINYINT:
        type = getTinyintType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.VARBINARY:
        type = getVarbinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.VARCHAR:
        type = getVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.NVARCHAR:
        type = getNationalVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      case Types.ARRAY:
        type = getArray(typeName, columnSize, decimalDigits, numPrecRadix, dataType);
        break;
      case Types.STRUCT:
        type = getComposedTypeStructure(database, currentSchema, dataType, typeName);
        break;
      case Types.OTHER:
        type = getOtherType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
      default:
        // tries to get specific DBMS data types
        type = getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }

    if (StringUtils.isBlank(type.getOriginalTypeName())) {
      type.setOriginalTypeName(typeName);
    }

    return type;
  }

  @Override
  protected Type getArray(String typeName, int columnSize, int decimalDigits, int numPrecRadix, int dataType)
    throws UnknownTypeException {
    Type subtype = getArraySubTypeFromTypeName(typeName, columnSize, decimalDigits, numPrecRadix, dataType);
    ComposedTypeArray type = new ComposedTypeArray(subtype);
    return type;
  }

  @Override
  protected Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    // TODO: add charset
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getTinyintType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getSmallIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getLongNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getIntegerType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    type.setSql99TypeName("INTEGER");
    type.setSql2008TypeName("INTEGER");
    return type;
  }

  @Override
  protected Type getClobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getNationalCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    // TODO add charset
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("CHARACTER", columnSize);
    type.setSql2008TypeName("CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("CHARACTER", columnSize);
    type.setSql2008TypeName("CHARACTER", columnSize);
    type.setOriginalTypeName("CHARACTER", columnSize);
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
    SimpleTypeBinary type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getBitType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if (columnSize > 1) {
      type = getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      type = new SimpleTypeBoolean();
      type.setSql99TypeName("BOOLEAN");
      type.setSql2008TypeName("BOOLEAN");
    }
    return type;
  }

  @Override
  protected Type getBigIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return getNumericType(typeName, columnSize, decimalDigits, numPrecRadix);
  }

  @Override
  protected Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema, int dataType,
    String typeName) {
    ComposedTypeStructure type = null;
    String schemaName = currentSchema.getName();

    SchemaStructure schema = currentSchema;
    if (schema == null) {
      schema = database.getSchemaByName(schemaName);
    }

    if (schema != null) {
      for (ComposedTypeStructure udt : schema.getUserDefinedTypes()) {
        if (udt.getOriginalTypeName().equalsIgnoreCase(typeName)) {
          type = udt;
          break;
        }
      }
    }

    if (type == null) {
      LOGGER.debug("Struct type could not be identified! Note that it may still be found later on. Schema name: "
        + schemaName + ", data type: " + dataType + ", type name=" + typeName);
      return ComposedTypeStructure.empty;
    } else {
      return type;
    }
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("REAL");
    type.setSql2008TypeName("REAL");
    return type;
  }

  @Override
  protected Type getArraySubTypeFromTypeName(String typeName, int columnSize, int decimalDigits, int numPrecRadix,
    int dataType) throws UnknownTypeException {
    Type subtype;
    if ("_char".equals(typeName)) {
      subtype = new SimpleTypeString(columnSize, false);
      subtype.setSql99TypeName("CHARACTER");
      subtype.setSql2008TypeName("CHARACTER");

    } else if ("_abstime".equals(typeName)) {
      subtype = getTimeType(typeName, columnSize, decimalDigits, numPrecRadix);
    } else {
      LOGGER.debug("Unsupported array datatype with code " + dataType);
      return new UnsupportedDataType(Types.ARRAY, typeName, columnSize, decimalDigits, numPrecRadix);
    }
    return subtype;
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    type.setOriginalTypeName(typeName, columnSize);
    return type;
  }

  @Override
  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(false, false);
    type.setSql99TypeName("DATE");
    type.setSql2008TypeName("DATE");
    return type;
  }

  @Override
  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    if (decimalDigits > 0) {
      type.setSql99TypeName("DECIMAL", columnSize, decimalDigits);
      type.setSql2008TypeName("DECIMAL", columnSize, decimalDigits);
    } else {
      type.setSql99TypeName("DECIMAL", columnSize);
      type.setSql2008TypeName("DECIMAL", columnSize);
    }
    return type;
  }

  @Override
  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericExact(columnSize, decimalDigits);
    if (decimalDigits > 0) {
      type.setSql99TypeName("NUMERIC", columnSize, decimalDigits);
      type.setSql2008TypeName("NUMERIC", columnSize, decimalDigits);
    } else {
      type.setSql99TypeName("NUMERIC", columnSize);
      type.setSql2008TypeName("NUMERIC", columnSize);
    }
    return type;
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("DOUBLE PRECISION");
    type.setSql2008TypeName("DOUBLE PRECISION");
    return type;
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeNumericApproximate(columnSize);
    type.setSql99TypeName("FLOAT");
    type.setSql2008TypeName("FLOAT");
    return type;
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
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(true, false);
    type.setSql99TypeName("TIME");
    type.setSql2008TypeName("TIME");
    return type;
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeDateTime(true, false);
    type.setSql99TypeName("TIMESTAMP");
    type.setSql2008TypeName("TIMESTAMP");
    return type;
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    return type;
  }

  /**
   * Gets data types defined as Types.OTHER. The data type is inferred by
   * typeName, sometimes specific to each DBMS
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param numPrecRadix
   * @param decimalDigits
   * @return the inferred data type
   * @throws UnknownTypeException
   */
  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    return getUnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
  }

  /**
   * Gets specific DBMS data types. E.g.:OracleTypes.BINARY_DOUBLE
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param numPrecRadix
   * @param decimalDigits
   * @return the inferred data type
   * @throws UnknownTypeException
   */
  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    return getUnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
  }

  /**
   * Gets the UnsupportedDataType. This data type is a placeholder for
   * unsupported data types
   *
   * @param dataType
   * @param typeName
   * @param columnSize
   * @param decimalDigits
   * @param numPrecRadix
   * @return
   * @throws UnknownTypeException
   */
  @Override
  protected Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException {
    LOGGER.debug("Unsupported JDBC type, code: " + dataType);
    return new UnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
  }
}
