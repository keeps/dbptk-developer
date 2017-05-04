package com.databasepreservation.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.DatabaseModuleFactory;

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

  private static Path homeDirectory, logDirectory, modulesDirectory, reportsDirectory;

  public static void initialize() {
    if (!initialized) {
      String homeDirectoryAsString = getProperty(Constants.DEFAULT_HOME_DIRECTORY, Constants.PROPERTY_KEY_HOME);
      initialize(Paths.get(homeDirectoryAsString).toAbsolutePath());
    }
  }

  public static void initialize(Path dbptkHome) {
    if (!initialized) {
      homeDirectory = dbptkHome;
      System.setProperty(Constants.PROPERTY_KEY_HOME, homeDirectory.toAbsolutePath().toString());

      logDirectory = homeDirectory.resolve(Constants.SUBDIRECTORY_LOG);
      modulesDirectory = homeDirectory.resolve(Constants.SUBDIRECTORY_MODULES);
      reportsDirectory = homeDirectory.resolve(Constants.SUBDIRECTORY_REPORTS);

      instantiateEssentialDirectories(homeDirectory, logDirectory, modulesDirectory, reportsDirectory);

      configureLogback();
      initialized = true;
    }
  }

  public static Path getModuleDirectory(DatabaseModuleFactory moduleFactory) {
    if (!initialized) {
      initialize();
    }

    Path moduleDirectory = modulesDirectory.resolve(moduleFactory.getModuleName().toLowerCase(Locale.ENGLISH));
    instantiateEssentialDirectories(moduleDirectory);
    return moduleDirectory;
  }

  public static Path getReportsDirectory() {
    return reportsDirectory;
  }

  private static void instantiateEssentialDirectories(Path... directories) {
    for (Path path : directories) {
      try {
        if (!Files.exists(path)) {
          Files.createDirectories(path);
        }
      } catch (IOException e) {
        LOGGER.error("Unable to create " + path, e);
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
      LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);
      context.reset();

      if (logbackFileResource != null) {
        configurator.doConfigure(logbackFileResource);
      }
    } catch (JoranException e) {
      LOGGER.error("Error configuring logback", e);
    }
  }

  public static String getVersionInfo() {
    InputStream versionInfoAsStream = ConfigUtils.class.getClassLoader().getSystemResourceAsStream(
      Constants.VERSION_INFO_FILE);
    String ret;
    try {
      ret = IOUtils.toString(versionInfoAsStream);
    } catch (IOException e) {
      LOGGER.debug("Could not obtain resource {}" + Constants.VERSION_INFO_FILE);
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
