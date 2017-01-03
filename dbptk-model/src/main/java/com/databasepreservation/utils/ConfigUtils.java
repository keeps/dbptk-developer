package com.databasepreservation.utils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Obtain values from system environment variables or properties.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ConfigUtils {
  private static final Map<String, String> retrievedProperties = new HashMap<>();

  private static String partsToEnvironment(String... parts) {
    return StringUtils.join(parts, '_').toUpperCase(Locale.ENGLISH);
  }

  private static String partsToProperty(String... parts) {
    return StringUtils.join(parts, '.').toLowerCase(Locale.ENGLISH);
  }

  /**
   * Same as getProperty(String defaultValue, String... parts), but attempts to
   * convert the String values to Integers via Integer.parseInt
   */
  public static Integer getProperty(Integer defaultValue, String... parts) {
    return Integer.parseInt(getProperty(defaultValue.toString(), parts));
  }

  /**
   * Get the value from the java properties (-Dxxx=yyy) or an environment
   * variable (in that order), ultimately returning the default value if one was
   * not found.
   * 
   * @param defaultValue
   *          the value to return if no other value is found
   * @param parts
   *          used to build the environment variable and property names. Ex:
   *          ["dbptk", "test"] becomes the environment variable "DBPTK_TEST"
   *          and the property "dbptk.test"
   * @return the value associated with the java property, OR the value
   *         associated with the environment variable, OR the default value (in
   *         this order).
   */
  public static String getProperty(String defaultValue, String... parts) {
    String propKey = partsToProperty(parts);

    String value = retrievedProperties.get(propKey);
    if (StringUtils.isNotBlank(value)) {
      return value;
    }

    value = System.getProperty(propKey, null);
    if (StringUtils.isNotBlank(value)) {
      retrievedProperties.put(propKey, value);
      return value;
    }

    String envKey = partsToEnvironment(parts);
    if (StringUtils.isNotBlank(value)) {
      return value;
    }

    value = System.getenv(envKey);
    if (StringUtils.isNotBlank(value)) {
      retrievedProperties.put(propKey, value);
      return value;
    }

    retrievedProperties.put(propKey, defaultValue);
    return defaultValue;
  }
}
