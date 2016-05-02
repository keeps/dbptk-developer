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
  protected Type getLongvarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if ("image".equals(typeName)) {
      type = new SimpleTypeBinary("MIME", "image");
    } else {
      type = new SimpleTypeBinary();
    }
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2003TypeName("BINARY LARGE OBJECT");
    return type;
  }
}
