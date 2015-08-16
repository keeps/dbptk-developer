package dk.magenta.siarddk;

import com.databasepreservation.modules.siard.common.sql99toXSDType;

public class SIARDDKsql99ToXsdType extends sql99toXSDType {

  public static String convert(String sql99Type) {

    if (sql99Type.equals("DOUBLE PRECISION")) {
      return "xs:decimal";
    } else if (sql99Type.equals("FLOAT")) {
      return "xs:decimal";
    } else if (sql99Type.equals("REAL")) {
      return "xs:decimal";
    }
    
    return null;
	}
}
