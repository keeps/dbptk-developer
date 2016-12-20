package com.databasepreservation.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to get application name and version information.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MiscUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MiscUtils.class);

  public static final String APP_VERSION = getProgramVersion();
  public static final String APP_NAME = "Database Preservation Toolkit";
  public static final String APP_NAME_AND_VERSION = APP_NAME + " (version " + APP_VERSION + ")";

  private static String getProgramVersion() {
    try {
      return MiscUtils.class.getPackage().getImplementationVersion();
    } catch (Exception e) {
      LOGGER.debug("Problem getting program version", e);
      return null;
    }
  }
}
