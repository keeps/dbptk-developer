package com.databasepreservation;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import com.databasepreservation.model.Reporter;
import org.apache.commons.cli.ParseException;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.modules.jdbc.JDBCModuleFactory;
import com.databasepreservation.modules.listTables.ListTablesModuleFactory;
import com.databasepreservation.modules.msAccess.MsAccessUCanAccessModuleFactory;
import com.databasepreservation.modules.mySql.MySQLModuleFactory;
import com.databasepreservation.modules.oracle.Oracle12cModuleFactory;
import com.databasepreservation.modules.postgreSql.PostgreSQLModuleFactory;
import com.databasepreservation.modules.siard.SIARD1ModuleFactory;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.siard.SIARDDKModuleFactory;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Main {
  public static final int EXIT_CODE_OK = 0;
  public static final int EXIT_CODE_GENERIC_ERROR = 1;
  public static final int EXIT_CODE_COMMAND_PARSE_ERROR = 2;
  public static final int EXIT_CODE_LICENSE_NOT_ACCEPTED = 3;
  public static final int EXIT_CODE_CONNECTION_ERROR = 4;
  public static final int EXIT_CODE_NOT_USING_UTF8 = 5;

  private static final String execID = UUID.randomUUID().toString();
  public static final String APP_VERSION = getProgramVersion();

  public static final String APP_NAME = "Database Preservation Toolkit";

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static final DatabaseModuleFactory[] databaseModuleFactories = new DatabaseModuleFactory[] {
    new JDBCModuleFactory(), new ListTablesModuleFactory(), new MsAccessUCanAccessModuleFactory(),
    new MySQLModuleFactory(), new Oracle12cModuleFactory(), new PostgreSQLModuleFactory(), new SIARD1ModuleFactory(),
    new SIARD2ModuleFactory(), new SIARDDKModuleFactory(), new SQLServerJDBCModuleFactory()};

  /**
   * @param args
   *          the console arguments
   */
  public static void main(String[] args) {
    CLI cli = new CLI(Arrays.asList(args), databaseModuleFactories);
    System.exit(internal_main(cli));
  }

  // used in testing
  public static int internal_main(String... args) {
    CLI cli = new CLI(Arrays.asList(args), databaseModuleFactories);
    return internal_main(cli);
  }

  public static int internal_main(CLI cli) {
    logProgramStart();
    cli.logOperativeSystemInfo();

    int exitStatus = EXIT_CODE_GENERIC_ERROR;
    if(cli.usingUTF8()) {
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
    }else{
      exitStatus = EXIT_CODE_NOT_USING_UTF8;
      LOGGER.info("The charset in use is not UTF-8.");
      LOGGER.info("Please try forcing UTF-8 charset by running the application with:");
      LOGGER.info("   java \"-Dfile.encoding=UTF-8\" -jar ...");
    }

    Reporter.finish();
    LOGGER.info("Please report any problems at https://github.com/keeps/db-preservation-toolkit/issues/new");
    logProgramFinish(exitStatus);

    return exitStatus;
  }

  private static int run(CLI cli) {
    DatabaseImportModule importModule;
    DatabaseExportModule exportModule;

    try {
      importModule = cli.getImportModule();
      exportModule = cli.getExportModule();
    } catch (ParseException e) {
      LOGGER.error(e.getMessage(), e);
      logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
      return EXIT_CODE_COMMAND_PARSE_ERROR;
    } catch (LicenseNotAcceptedException e) {
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
      } else if ("SQL error while conecting".equalsIgnoreCase(e.getMessage())) {
        LOGGER.error("Connection error while importing/exporting", e);
        exitStatus = EXIT_CODE_CONNECTION_ERROR;
      }

      if (e.getModuleErrors() != null) {
        for (Map.Entry<String, Throwable> entry : e.getModuleErrors().entrySet()) {
          LOGGER.error(entry.getKey(), entry.getValue());
        }
      } else {
        LOGGER.error("Error while importing/exporting", e);
      }
    } catch (UnknownTypeException e) {
      LOGGER.error("Error while importing/exporting", e);
    } catch (InvalidDataException e) {
      LOGGER.error("Error while importing/exporting", e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception", e);
    }
    return exitStatus;
  }

  private static void logProgramStart() {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   START-ID-" + execID);
    LOGGER.debug("#   START v" + APP_VERSION);
    LOGGER.debug("#########################################################");
  }

  private static void logProgramFinish(int exitStatus) {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   FINISH-ID-" + execID);
    LOGGER.debug("#   FINISH v" + APP_VERSION);
    LOGGER.debug("#   EXIT CODE: " + exitStatus);
    LOGGER.debug("#########################################################");
  }

  private static String getProgramVersion() {
    try {
      return Main.class.getPackage().getImplementationVersion();
    } catch (Exception e) {
      LOGGER.debug("Problem getting program version", e);
      return null;
    }
  }
}
