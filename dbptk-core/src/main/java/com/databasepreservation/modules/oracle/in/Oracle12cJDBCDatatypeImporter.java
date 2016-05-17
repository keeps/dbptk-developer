package com.databasepreservation.modules.oracle.in;

import com.databasepreservation.model.exception.UnknownTypeException;
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
  @Override
  protected Type getLongvarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException("Unsuported JDBC type, code: -1. Oracle " + typeName
      + " data type is not supported.");
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
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    // TODO define charset
    if ("NCHAR".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), false, "CHARSET");
      type.setSql99TypeName("CHARACTER");
      type.setSql2008TypeName("CHARACTER");
    } else if ("NVARCHAR2".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true, "CHARSET");
      type.setSql99TypeName("CHARACTER VARYING", columnSize);
      type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    } else if ("NCLOB".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(Integer.valueOf(columnSize), true, "CHARSET");
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2008TypeName("CHARACTER LARGE OBJECT");
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
      // case OracleTypes.BFILE:
      // type = new SimpleTypeBinary();
      // break;
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
}
