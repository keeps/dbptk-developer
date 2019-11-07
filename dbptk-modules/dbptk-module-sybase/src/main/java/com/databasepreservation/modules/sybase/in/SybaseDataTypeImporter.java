/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sybase.in;

import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SybaseDataTypeImporter extends JDBCDatatypeImporter {
  private static final String TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
  private static final String TIMESTAMP = "TIMESTAMP";

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if (TIMESTAMP_WITH_TIME_ZONE.equalsIgnoreCase(typeName)) {
      type = new SimpleTypeDateTime(true, true);
      type.setSql99TypeName(TIMESTAMP_WITH_TIME_ZONE);
      type.setSql2008TypeName(TIMESTAMP_WITH_TIME_ZONE);
    } else {
      type = new SimpleTypeDateTime(true, false);
      type.setSql99TypeName(TIMESTAMP);
      type.setSql2008TypeName(TIMESTAMP);
    }

    return type;
  }
}
