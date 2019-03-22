/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sqlServer.in;

import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class SQLServerDatatypeImporter extends JDBCDatatypeImporter {
  @Override
  protected Type getLongVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("image".equals(typeName)) {
      type = new SimpleTypeBinary("MIME", "image");
    } else {
      type = new SimpleTypeBinary();
    }
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.equalsIgnoreCase("timestamp") || typeName.equalsIgnoreCase("rowversion")) {
      return getVarcharType(typeName, columnSize, decimalDigits, numPrecRadix);
    }
    return super.getBinaryType(typeName, columnSize, decimalDigits, numPrecRadix);
  }
}
