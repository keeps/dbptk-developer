package com.databasepreservation.modules.siard.in.metadata.typeConverter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class TypeConverterFactory {
  private static TypeConverter sql99;

  public static TypeConverter getSQL99TypeConverter() {
    if (sql99 == null) {
      sql99 = new SQL99TypeConverter();
    }
    return sql99;
  }
}
