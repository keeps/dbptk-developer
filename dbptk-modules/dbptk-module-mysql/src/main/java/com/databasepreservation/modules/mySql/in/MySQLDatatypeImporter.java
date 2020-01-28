/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.mySql.in;

import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class MySQLDatatypeImporter extends JDBCDatatypeImporter {
  @Override
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 12 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("REAL");
      type.setSql2008TypeName("REAL");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 22 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("DOUBLE PRECISION");
      type.setSql2008TypeName("DOUBLE PRECISION");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY", columnSize);
    type.setSql2008TypeName("BINARY", columnSize);
    type.setOriginalTypeName(typeName, columnSize);
    return type;
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT VARYING", columnSize * 8);
    type.setSql2008TypeName("BINARY VARYING", columnSize * 8);
    return type;
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;

    if (columnSize == 12 && decimalDigits == 0) {
      type = new SimpleTypeNumericApproximate(columnSize);
      type.setSql99TypeName("FLOAT");
      type.setSql2008TypeName("FLOAT");
    } else {
      type = getDecimalType(typeName, columnSize, decimalDigits, numPrecRadix);
    }

    return type;
  }

  @Override
  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if ("YEAR".equals(typeName)) {
      return getNumericType(typeName, 4, decimalDigits, numPrecRadix);
    } else {
      return super.getDateType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
  }
}
