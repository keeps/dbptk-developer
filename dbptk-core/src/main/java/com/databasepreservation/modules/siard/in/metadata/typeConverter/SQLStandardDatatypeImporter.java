package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.modules.DatatypeImporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class SQLStandardDatatypeImporter extends DatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLStandardDatatypeImporter.class.getName());

  private static final int DEFAULT_COLUMN_SIZE = 0;
  private static final int DEFAULT_DECIMAL_DIGITS = 0;
  private static final int DEFAULT_NUM_PREC_RADIX = 10;

  /**
   * getCheckedType method, simplified to use with SIARD import modules
   */
  public Type getCheckedType(String databaseName, String schemaName, String tableName, String columnName,
    String sqlStandardType) {
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
        columnSize = Integer.parseInt(sqlStandardType.substring(indexOfOpen, indexOfComma).trim());
        decimalDigits = Integer.parseInt(sqlStandardType.substring(indexOfComma, indexOfClose).trim());
      } else if (indexOfOpen >= 0 && indexOfClose >= 0) {
        // format like NAME(PARAM1)
        columnSize = Integer.parseInt(sqlStandardType.substring(indexOfOpen, indexOfComma).trim());
      }
    } catch (NumberFormatException e) {
      columnSize = DEFAULT_COLUMN_SIZE;
      decimalDigits = DEFAULT_DECIMAL_DIGITS;
    }

    return getCheckedType(database, schema, tableName, columnName, 0, sqlStandardType, columnSize, decimalDigits, numPrecRadix);
  }
}
