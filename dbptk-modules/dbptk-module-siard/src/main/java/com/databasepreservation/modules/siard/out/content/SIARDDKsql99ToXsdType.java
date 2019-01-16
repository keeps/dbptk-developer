/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
    } else if (sql99Type.startsWith("BOOLEAN")) {
      return "xs:boolean";
    } else if (sql99Type.startsWith("CHARACTER") || sql99Type.startsWith("NATIONAL CHARACTER")
      || sql99Type.equals(SIARDDKConstants.CHARACTER_LARGE_OBJECT)) {
      return "xs:string";
    } else if (sql99Type.startsWith("DATE")) {
      return "xs:date";
    } else if (sql99Type.startsWith("INTEGER") || sql99Type.startsWith("SMALLINT")
      || sql99Type.equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
      return "xs:integer";
    } else if ("TIME".equals(sql99Type) || "TIME WITH TIME ZONE".equals(sql99Type)) {
      return "xs:time";
    } else if ("TIMESTAMP".equals(sql99Type) || "TIMESTAMP WITH TIME ZONE".equals(sql99Type)) {
      return "xs:dateTime";
    } else if (sql99Type.startsWith("BIT VARYING")) {
      return "xs:hexBinary";
    } else if (sql99Type.startsWith("BINARY VARYING")) {
      return "xs:hexBinary";
    }

    return null;

  }

}
