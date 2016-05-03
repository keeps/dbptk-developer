package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import java.sql.SQLException;
import java.util.Locale;

import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQL99StandardDatatypeImporter extends SQLStandardDatatypeImporter {
  @Override
  protected Type getType(DatabaseStructure database, SchemaStructure currentSchema, String tableName,
    String columnName, int dataType, String sql99TypeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException, SQLException, ClassNotFoundException {

    // obtain the type without parameter information
    String typeNameWithoutParameters;
    int leftParenthesisIndex = sql99TypeName.indexOf('(');
    int rightParenthesisIndex = sql99TypeName.lastIndexOf(')');
    if (leftParenthesisIndex >= 0 && rightParenthesisIndex >= 0) {
      if (rightParenthesisIndex >= sql99TypeName.length()) {
        rightParenthesisIndex = sql99TypeName.length() - 1;
      }
      typeNameWithoutParameters = sql99TypeName.substring(0, leftParenthesisIndex)
        + sql99TypeName.substring(rightParenthesisIndex + 1);
    } else {
      typeNameWithoutParameters = sql99TypeName;
    }
    typeNameWithoutParameters = typeNameWithoutParameters.toUpperCase(Locale.ENGLISH);

    Type type;
    switch (typeNameWithoutParameters) {
      case "CHARACTER":
      case "CHAR":
        type = getCharType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "NATIONAL CHARACTER":
      case "NATIONAL CHAR":
      case "NCHAR":
        type = getNationalCharType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "CHARACTER VARYING":
      case "VARCHAR":
        type = getVarcharType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "NATIONAL CHARACTER VARYING":
      case "NATIONAL CHAR VARYING":
      case "NCHAR VARYING":
        type = getNationalVarcharType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "CHARACTER LARGE OBJECT":
      case "CHAR LARGE OBJECT":
      case "CLOB":
      case "NATIONAL CHARACTER LARGE OBJECT":
      case "NCHAR LARGE OBJECT":
      case "NCLOB":
        type = getClobType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "BINARY":
        type = getBinaryType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "BINARY VARYING":
      case "VARBINARY":
        type = getVarbinaryType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "BINARY LARGE OBJECT":
      case "BLOB":
        type = getBlobType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "NUMERIC":
        type = getNumericType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "DECIMAL":
      case "DEC":
        type = getDecimalType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "SMALLINT":
        type = getSmallIntType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "INTEGER":
      case "INT":
        type = getIntegerType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "BIGINT":
        type = getBigIntType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "FLOAT":
        type = getFloatType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "REAL":
        type = getRealType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "DOUBLE PRECISION":
        type = getDoubleType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "BOOLEAN":
        type = getBooleanType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "DATE":
        type = getDateType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "TIME":
      case "TIME WITH TIME ZONE":
      case "TIME WITHOUT TIME ZONE":
        type = getTimeType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      case "TIMESTAMP":
      case "TIMESTAMP WITH TIME ZONE":
      case "TIMESTAMP WITHOUT TIME ZONE":
        type = getTimestampType(typeNameWithoutParameters, columnSize, decimalDigits, numPrecRadix);
        break;

      default:
        type = getFallbackType(sql99TypeName);
    }

    // exceptions
    // TODO: add support for more types

    if (StringUtils.isBlank(type.getSql99TypeName())) {
      type.setSql99TypeName(sql99TypeName);
    }

    if (StringUtils.isBlank(type.getSql2003TypeName())) {
      // TODO: convert sql99 to sql2003
    }

    type.setOriginalTypeName(sql99TypeName);
    return type;
  }

  @Override
  protected Type getArray(String typeName, int columnSize, int decimalDigits, int numPrecRadix, int dataType)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeString(columnSize, true);
  }

  @Override
  protected Type getTinyintType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getSmallIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getLongNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeString(columnSize, true);
  }

  @Override
  protected Type getIntegerType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getClobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (columnSize != 0) {
      return new SimpleTypeString(columnSize, true);
    } else {
      return new SimpleTypeString(Integer.MAX_VALUE, true);
    }
  }

  @Override
  protected Type getNationalCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeString(columnSize, true);
  }

  @Override
  protected Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeString(columnSize, true);
  }

  @Override
  protected Type getBooleanType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeBoolean();
  }

  @Override
  protected Type getBlobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (columnSize != 0) {
      return new SimpleTypeBinary(columnSize);
    } else {
      return new SimpleTypeBinary();
    }
  }

  @Override
  protected Type getBitType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getBigIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema, int dataType,
    String typeName) {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeBinary(columnSize);
  }

  @Override
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(columnSize);
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
    return new SimpleTypeNumericApproximate(columnSize);
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(columnSize);
  }

  @Override
  protected Type getLongvarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeBinary(columnSize);
  }

  @Override
  protected Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    if (columnSize != 0) {
      return new SimpleTypeString(columnSize, true);
    } else {
      return new SimpleTypeString(Integer.MAX_VALUE, true);
    }
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if(typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    }else{
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if(typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    }else{
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeString(columnSize, true);
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
}
