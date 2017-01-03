package com.databasepreservation.utils;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Obtain values from system environment variables.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ConfigUtils {
  private static final Map<String, String> retrievedProperties = new HashMap<>();

  private static String partsToEnvironment(String... parts) {
    return StringUtils.join(parts, '_').toUpperCase(Locale.ENGLISH);
  }

  public static Integer getProperty(Integer defaultValue, String... parts) {
    return Integer.parseInt(getProperty(defaultValue.toString(), parts));
  }

  public static String getProperty(String defaultValue, String... parts) {
    String envKey = partsToEnvironment(parts);

    String value = retrievedProperties.get(envKey);
    if (StringUtils.isNotBlank(value)) {
      return value;
    }

    value = System.getenv(envKey);
    if (StringUtils.isNotBlank(value)) {
      retrievedProperties.put(envKey, value);
      return value;
    }

    retrievedProperties.put(envKey, defaultValue);
    return defaultValue;
  }
}
