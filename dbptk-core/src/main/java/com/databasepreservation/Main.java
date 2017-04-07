package com.databasepreservation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.modules.dbml.DBMLModuleFactory;
import com.databasepreservation.modules.jdbc.JDBCModuleFactory;
import com.databasepreservation.modules.listTables.ListTablesModuleFactory;
import com.databasepreservation.modules.msAccess.MsAccessUCanAccessModuleFactory;
import com.databasepreservation.modules.mySql.MySQLModuleFactory;
import com.databasepreservation.modules.oracle.Oracle12cModuleFactory;
import com.databasepreservation.modules.postgreSql.PostgreSQLModuleFactory;
import com.databasepreservation.modules.siard.SIARD1ModuleFactory;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.siard.SIARDDKModuleFactory;
import com.databasepreservation.modules.solr.SolrModuleFactory;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import com.databasepreservation.utils.ConfigUtils;
import com.databasepreservation.utils.MiscUtils;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Main {
  static {
    // initialize logging
    ConfigUtils.initialize();
  }

  public static final int EXIT_CODE_OK = 0;
  public static final int EXIT_CODE_GENERIC_ERROR = 1;
  public static final int EXIT_CODE_COMMAND_PARSE_ERROR = 2;
  public static final int EXIT_CODE_LICENSE_NOT_ACCEPTED = 3;
  public static final int EXIT_CODE_CONNECTION_ERROR = 4;
  public static final int EXIT_CODE_NOT_USING_UTF8 = 5;

  private static final String execID = UUID.randomUUID().toString();

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static Reporter reporter = null;

  private static Reporter getReporter() {
    if (reporter == null) {
      reporter = new Reporter();
    }
    return reporter;
  }

  private static DatabaseModuleFactory[] databaseModuleFactories = null;

  private static DatabaseModuleFactory[] getDatabaseModuleFactories() {
    if (databaseModuleFactories == null) {
      databaseModuleFactories = new DatabaseModuleFactory[] {new JDBCModuleFactory(getReporter()),
        new ListTablesModuleFactory(getReporter()), new MsAccessUCanAccessModuleFactory(getReporter()),
        new MySQLModuleFactory(getReporter()), new Oracle12cModuleFactory(getReporter()),
        new PostgreSQLModuleFactory(getReporter()), new SIARD1ModuleFactory(getReporter()),
        new SIARD2ModuleFactory(getReporter()), new SIARDDKModuleFactory(getReporter()),
        new SolrModuleFactory(getReporter()), new SQLServerJDBCModuleFactory(getReporter()),
        new DBMLModuleFactory(getReporter())};
    }
    return databaseModuleFactories;
  }

  /**
   * @param args
   *          the console arguments
   */
  public static void main(String[] args) {
    CLI cli = new CLI(Arrays.asList(args), getDatabaseModuleFactories());
    System.exit(internal_main(cli));
  }

  // used in testing
  public static int internal_main(String... args) {
    CLI cli = new CLI(Arrays.asList(args), getDatabaseModuleFactories());
    return internal_main(cli);
  }

  public static int internal_main(CLI cli) {
    logProgramStart();
    cli.logOperatingSystemInfo();

    // avoid SAX processing limit of 50 million elements
    System.setProperty("totalEntitySizeLimit", "0");
    System.setProperty("jdk.xml.totalEntitySizeLimit", "0");

    int exitStatus = EXIT_CODE_GENERIC_ERROR;
    if (cli.usingUTF8()) {
      if (cli.shouldPrintHelp()) {
        cli.printHelp();
      } else {
        exitStatus = run(cli);
        if (exitStatus == EXIT_CODE_CONNECTION_ERROR) {
          LOGGER.info("Disabling connection encryption (for modules that support it) and trying again.");
          cli.disableEncryption();
          exitStatus = run(cli);
        }
      }
    } else {
      exitStatus = EXIT_CODE_NOT_USING_UTF8;
      LOGGER.error("The charset in use is not UTF-8.");
      LOGGER.info("Please try forcing UTF-8 charset by running the application with:");
      LOGGER.info("   java \"-Dfile.encoding=UTF-8\" -jar ...");
    }

    try {
      getReporter().close();
    } catch (IOException e) {
      LOGGER.debug("There was a problem closing the report file.", e);
    }
    LOGGER.info("Troubleshooting information can be found at http://www.database-preservation.com/#troubleshooting");
    LOGGER.info("Please report any problems at https://github.com/keeps/db-preservation-toolkit/issues/new");
    logProgramFinish(exitStatus);

    return exitStatus;
  }

  private static int run(CLI cli) {
    DatabaseImportModule importModule;
    DatabaseExportModule exportModule;

    try {
      importModule = cli.getImportModule();
      importModule.setOnceReporter(getReporter());
      exportModule = cli.getExportModule();
      exportModule.setOnceReporter(getReporter());
    } catch (ParseException e) {
      LOGGER.error(e.getMessage(), e);
      logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
      return EXIT_CODE_COMMAND_PARSE_ERROR;
    } catch (LicenseNotAcceptedException e) {
      LOGGER.trace("LicenseNotAcceptedException", e);
      LOGGER.info("The license must be accepted to use this module.");
      LOGGER.info("==================================================");
      LOGGER.info(e.getLicense());
      LOGGER.info("==================================================");
      logProgramFinish(EXIT_CODE_LICENSE_NOT_ACCEPTED);
      return EXIT_CODE_LICENSE_NOT_ACCEPTED;
    }

    int exitStatus = EXIT_CODE_GENERIC_ERROR;
    try {
      long startTime = System.currentTimeMillis();
      LOGGER.info("Converting database: " + cli.getImportModuleName() + " to " + cli.getExportModuleName());
      importModule.getDatabase(exportModule);
      long duration = System.currentTimeMillis() - startTime;
      LOGGER.info("Run time " + (duration / 60000) + "m " + (duration % 60000 / 1000) + "s");
      exitStatus = EXIT_CODE_OK;
    } catch (ModuleException e) {
      if (e.getCause() != null && e.getCause() instanceof ClassNotFoundException
        && "sun.jdbc.odbc.JdbcOdbcDriver".equals(e.getCause().getMessage())) {
        LOGGER.error("Could not find the Java ODBC driver, "
          + "please run this program under Windows to use the JDBC-ODBC bridge.", e.getCause());
      } else if ("SQL error while connecting".equalsIgnoreCase(e.getMessage())) {
        LOGGER.error("Connection error while importing/exporting", e);
        exitStatus = EXIT_CODE_CONNECTION_ERROR;
      }

      if (e.getModuleErrors() != null) {
        for (Map.Entry<String, Throwable> entry : e.getModuleErrors().entrySet()) {
          LOGGER.error(entry.getKey(), entry.getValue());
        }
      } else {
        LOGGER.error("Fatal error while converting the database (" + e.getMessage() + ")", e);
      }
    } catch (UnknownTypeException e) {
      LOGGER.error("Fatal error while converting the database (" + e.getMessage() + ")", e);
    } catch (Exception e) {
      LOGGER.error("Fatal error: Unexpected exception (" + e.getMessage() + ")", e);
    }
    return exitStatus;
  }

  private static void logProgramStart() {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   START-ID-" + execID);
    LOGGER.debug("#   START v" + MiscUtils.APP_VERSION);
    LOGGER.debug("#########################################################");
  }

  private static void logProgramFinish(int exitStatus) {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   FINISH-ID-" + execID);
    LOGGER.debug("#   FINISH v" + MiscUtils.APP_VERSION);
    LOGGER.debug("#   EXIT CODE: " + exitStatus);
    LOGGER.debug("#########################################################");
  }
}
