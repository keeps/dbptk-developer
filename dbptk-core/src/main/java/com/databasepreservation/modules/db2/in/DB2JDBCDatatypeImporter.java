package com.databasepreservation.modules.db2.in;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Miguel Coutada
 */
public class DB2JDBCDatatypeImporter extends JDBCDatatypeImporter {
  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    if ("XML".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeString(31457280, true);
      type.setSql99TypeName("CHARACTER LARGE OBJECT");
      type.setSql2003TypeName("CHARACTER LARGE OBJECT");
    } else if ("DECFLOAT".equalsIgnoreCase(typeName)) {
      type = new SimpleTypeNumericApproximate(Integer.valueOf(columnSize));
      type.setSql99TypeName("DOUBLE PRECISION");
      type.setSql2003TypeName("DOUBLE PRECISION");
    } else {
      type = super.getOtherType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
    }
    return type;
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    switch (dataType) {
      // case 2001:
      // type = new SimpleTypeNumericApproximate(
      // Integer.valueOf(columnSize));
      // break;
      default:
        type = super.getSpecificType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);
        break;
    }
    return type;
  }
}
