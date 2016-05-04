package com.databasepreservation.model.modules;

import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * Converts a datatype from a source database to a Type object
 * 
 * Code using this class or any of its subclasses should create a new instance
 * and use the getCheckedType method, to convert a type and Report any potential
 * problems.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class DatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatatypeImporter.class);

  private static final int FALLBACK_TYPE_SIZE = 65535;

  /**
   * Map the original type to the normalized type model
   *
   * @param tableName
   *          The name of the associated table, needed to resolve user defined
   *          data types
   * @param columnName
   *          The name of the associated column, needed to resolve user defined
   *          data types
   * @param dataType
   *          the JDBC identifier of the original datatype
   * @param typeName
   *          the name of the original data type
   * @param columnSize
   *          the column size for the type
   * @param decimalDigits
   *          the number of decimal digits for the type
   * @param numPrecRadix
   *          Indicates the numeric radix of this data type, which is usually 2
   *          or 10
   * @return the normalized type
   * @throws UnknownTypeException
   *           the original type is unknown and cannot be mapped
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public Type getCheckedType(DatabaseStructure database, SchemaStructure currentSchema, String tableName,
    String columnName, int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix) {

    Type type = getFallbackType(typeName);
    try {
      type = getType(database, currentSchema, tableName, columnName, dataType, typeName, columnSize, decimalDigits,
        numPrecRadix);
    } catch (UnknownTypeException e) {
      LOGGER.debug("Got an UnknownTypeException while getting the source database type", e);
    } catch (SQLException e) {
      LOGGER.debug("Got an SQLException while getting the source database type", e);
    } catch (ClassNotFoundException e) {
      LOGGER.debug("Got an ClassNotFoundException while getting the source database type", e);
    }

    checkType(type, database, currentSchema, tableName, columnName, dataType, typeName, columnSize, decimalDigits,
      numPrecRadix);

    return type;
  }

  /**
   * Checks the type for problems and reports them
   *
   * @param type
   *          the type to check
   */
  private void checkType(Type type, DatabaseStructure database, SchemaStructure currentSchema, String tableName,
    String columnName, int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (StringUtils.isBlank(type.getSql99TypeName())) {
      // LOGGER.debug("Could not get SQL99 type for type: " + type.toString());
      // TODO add this in reporter
    }

    if (StringUtils.isBlank(type.getSql2003TypeName())) {
      // LOGGER.debug("Could not get SQL2003 type for type: " +
      // type.toString());
      // TODO add this in reporter
    }

    Reporter.dataTypeChangedOnImport(this.getClass().getName(), currentSchema.getName(), tableName, columnName, type);
  }

  protected Type getFallbackType(String originalTypeName) {
    Type type = new SimpleTypeString(FALLBACK_TYPE_SIZE, true);
    if (StringUtils.isNotBlank(originalTypeName)) {
      type.setOriginalTypeName(originalTypeName);
    } else {
      type.setOriginalTypeName("UNKNOWN");
    }
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  protected abstract Type getType(DatabaseStructure database, SchemaStructure currentSchema, String tableName,
    String columnName, int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException, SQLException, ClassNotFoundException;

  protected abstract Type getArray(String typeName, int columnSize, int decimalDigits, int numPrecRadix, int dataType)
    throws UnknownTypeException;

  protected abstract Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getTinyintType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getSmallIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getLongNationalVarcharType(String typeName, int columnSize, int decimalDigits,
    int numPrecRadix);

  protected abstract Type getIntegerType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getClobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getNationalCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getBooleanType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getBlobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getBitType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getBigIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema,
    int dataType, String typeName);

  protected abstract Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getArraySubTypeFromTypeName(String typeName, int columnSize, int decimalDigits,
    int numPrecRadix, int dataType) throws UnknownTypeException;

  protected abstract Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getLongvarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException;

  protected abstract Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

  protected abstract Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix);

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
  protected abstract Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException;

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
  protected abstract Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException;

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
  protected abstract Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException;
}
