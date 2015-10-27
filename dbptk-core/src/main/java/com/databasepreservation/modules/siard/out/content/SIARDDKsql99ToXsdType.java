package com.databasepreservation.modules.siard.out.content;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKsql99ToXsdType {

  public static String convert(String sql99Type) {

    if (sql99Type.startsWith("NUMERIC") || sql99Type.startsWith("DECIMAL") || sql99Type.startsWith("DOUBLE PRECISION")
      || sql99Type.startsWith("FLOAT") || sql99Type.startsWith("REAL")) {
      return "xs:decimal";
    } else if (sql99Type.startsWith("BIT")) {
      // Not in SIARDDK
      return "xs:hexBinary";
    } else if (sql99Type.startsWith("BOOLEAN")) {
      return "xs:boolean";
    } else if (sql99Type.startsWith("CHARACTER") || sql99Type.equals(SIARDDKConstants.CHARACTER_LARGE_OBJECT)) {
      return "xs:string";
    } else if (sql99Type.startsWith("DATE")) {
      return "xs:date";
    } else if (sql99Type.startsWith("INTEGER") || sql99Type.startsWith("SMALLINT")
      || sql99Type.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
      return "xs:integer";
    } else if (sql99Type.equals("TIME") || sql99Type.equals("TIME WITH TIME ZONE")) {
      return "xs:time";
    } else if (sql99Type.equals("TIMESTAMP") || sql99Type.equals("TIMESTAMP WITH TIME ZONE")) {
      return "xs:dateTime";
    }

    return null;

  }

}
