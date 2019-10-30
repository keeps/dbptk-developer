/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

/**
 * Obtain values from system environment variables or properties.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ConfigUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);
  private static boolean initialized = false;

  private static Path homeDirectory;
  private static Path hiddenHomeDirectory;
  private static Path mapDBHomeDirectory;

  public static void initialize() {
    if (!initialized) {
      Path defaultHome;
      Path hiddenHome = Paths.get(System.getProperty("user.home"), ".dbptk").normalize().toAbsolutePath();

      Path jarPath = FileUtils.getJarPath();
      if (jarPath != null) {
        defaultHome = jarPath.getParent();
      } else {
        try {
          defaultHome = Files.createTempDirectory("dbptk_home_");
        } catch (IOException e) {
          LOGGER.debug("Could not create dbptk home in temporary folder", e);
          defaultHome = Paths.get(".");
        }
      }

      String homeDirectoryAsString = getProperty(defaultHome.toString(), Constants.PROPERTY_KEY_HOME);

      initialize(Paths.get(homeDirectoryAsString).toAbsolutePath(), hiddenHome);
    }
  }

  public static void initialize(Path dbptkHome, Path hiddenHome) {
    if (!initialized) {
      homeDirectory = dbptkHome;
      System.setProperty(Constants.PROPERTY_KEY_HOME, homeDirectory.toAbsolutePath().toString());

      hiddenHomeDirectory = hiddenHome;
      System.setProperty(Constants.PROPERTY_KEY_HIDDEN_HOME, hiddenHomeDirectory.toAbsolutePath().toString());

      mapDBHomeDirectory = Paths.get(hiddenHome.toAbsolutePath().toString(), Constants.MAPDB_FOLDER);

      instantiateEssentialDirectories(homeDirectory, hiddenHomeDirectory, mapDBHomeDirectory);

      configureLogback();
      initialized = true;
    }
  }

  public static Path getReportsDirectory() {
    return homeDirectory;
  }

  public static Path getHomeDirectory() {
    return homeDirectory;
  }

  public static Path getHiddenHomeDirectory() { return hiddenHomeDirectory; }

  public static Path getMapDBHomeDirectory() { return mapDBHomeDirectory; }

  private static void instantiateEssentialDirectories(Path... directories) {
    for (Path path : directories) {
      try {
        if (!Files.exists(path)) {
          LOGGER.debug("CREATING: {}", path);
          Files.createDirectories(path);
        }
      } catch (IOException e) {
        LOGGER.error("Unable to create {}", path, e);
      }
    }
  }

  private static void configureLogback() {
    // 20170314 hsilva: logback file was named differently from what logback
    // usually expects in order to avoid auto-loading by logback as we want to
    // place the log file under dbptk home
    // 20170407 bferreira: since logback.xml is only available in dbptk-core, we
    // should check if it present. If our logback file is not present, just
    // leave logback to use whichever defaults are in place (since DBPTK are
    // used in a modular way)
    URL logbackFileResource = ClassLoader.getSystemResource(Constants.LOGBACK_FILE_NAME);

    try {
      if (logbackFileResource != null) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        context.reset();
        configurator.doConfigure(logbackFileResource);
      }
    } catch (JoranException e) {
      LOGGER.error("Error configuring logback", e);
    }
  }

  public static String getVersionInfo() {
    InputStream versionInfoAsStream = ConfigUtils.class.getClassLoader()
      .getSystemResourceAsStream(Constants.VERSION_INFO_FILE);
    String ret;
    try {
      ret = IOUtils.toString(versionInfoAsStream);

    } catch (IOException e) {
      LOGGER.debug("Could not obtain resource {}", Constants.VERSION_INFO_FILE);
      ret = "<unavailable>";
    } finally {
      IOUtils.closeQuietly(versionInfoAsStream);
    }
    return ret;
  }

  /**
   * Same as getProperty, but attempts to convert the String values to Integers
   * via Integer.parseInt
   */
  public static Integer getProperty(Integer defaultValue, String propertyKey) {
    return Integer.parseInt(getProperty(defaultValue.toString(), propertyKey));
  }

  /**
   * Get the value from the java properties (-Dxxx=yyy) and return the default
   * value if the property is not defined.
   *
   * @param defaultValue
   *          the value to return if no other value is found
   * @param propertyKey
   *          The java property key
   * @return the value associated with the java property, or the default value.
   */
  public static String getProperty(String defaultValue, String propertyKey) {
    String value = System.getProperty(propertyKey, null);
    return StringUtils.isNotBlank(value) ? value : defaultValue;
  }
}
