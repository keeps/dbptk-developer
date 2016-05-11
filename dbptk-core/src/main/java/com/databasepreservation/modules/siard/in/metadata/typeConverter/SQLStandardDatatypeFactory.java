package com.databasepreservation.modules.siard.in.metadata.typeConverter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLStandardDatatypeFactory {
  private static SQLStandardDatatypeImporter sql99;
  private static SQLStandardDatatypeImporter sql2008;

  public static SQLStandardDatatypeImporter getSQL99StandardDatatypeImporter() {
    if (sql99 == null) {
      sql99 = new SQL99StandardDatatypeImporter();
    }
    return sql99;
  }

  public static SQLStandardDatatypeImporter getSQL2008StandardDatatypeImporter() {
    if (sql2008 == null) {
      sql2008 = new SQL2008StandardDatatypeImporter();
    }
    return sql2008;
  }
}
