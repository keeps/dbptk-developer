package com.databasepreservation.utils;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SanitizeUtils {
  public SanitizeUtils() {
  }

  public static String sanitizeName(String name) {
    return name.replaceAll("[^a-zA-Z0-9-_.~]", "");
  }
}
