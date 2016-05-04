package com.databasepreservation.modules.siard.in.metadata.typeConverter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLStandardDatatypeFactory {
  private static SQLStandardDatatypeImporter sql99;
  private static SQLStandardDatatypeImporter sql2003;

  public static SQLStandardDatatypeImporter getSQL99StandardDatatypeImporter() {
    if (sql99 == null) {
      sql99 = new SQL99StandardDatatypeImporter();
    }
    return sql99;
  }

  public static SQLStandardDatatypeImporter getSQL2003StandardDatatypeImporter() {
    if (sql2003 == null) {
      sql2003 = new SQL2003StandardDatatypeImporter();
    }
    return sql2003;
  }
}
