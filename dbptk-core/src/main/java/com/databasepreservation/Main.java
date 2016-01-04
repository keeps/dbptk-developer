package com.databasepreservation;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

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
import com.databasepreservation.modules.msAccess.MsAccessUCanAccessModuleFactory;
import com.databasepreservation.modules.mySql.MySQLModuleFactory;
import com.databasepreservation.modules.oracle.Oracle12cModuleFactory;
import com.databasepreservation.modules.postgreSql.PostgreSQLModuleFactory;
import com.databasepreservation.modules.siard.SIARD1ModuleFactory;
import com.databasepreservation.modules.siard.SIARD2ModuleFactory;
import com.databasepreservation.modules.siard.SIARDDKModuleFactory;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Main {
  public static final int EXIT_CODE_OK = 0;
  public static final int EXIT_CODE_GENERIC_ERROR = 1;
  public static final int EXIT_CODE_COMMAND_PARSE_ERROR = 2;
  public static final int EXIT_CODE_LICENSE_NOT_ACCEPTED = 3;

  private static final String execID = UUID.randomUUID().toString();
  public static final String APP_VERSION = getProgramVersion();

  public static final String APP_NAME = "db-preservation-toolkit - KEEP SOLUTIONS";

  public static final String NAME = "db-preservation-toolkit";

  private static final CustomLogger logger = CustomLogger.getLogger(Main.class);

  public static final DatabaseModuleFactory[] databaseModuleFactories = new DatabaseModuleFactory[] {
    new JDBCModuleFactory(), new MsAccessUCanAccessModuleFactory(), new MySQLModuleFactory(),
    new Oracle12cModuleFactory(), new PostgreSQLModuleFactory(), new SIARD1ModuleFactory(), new SIARD2ModuleFactory(),
    new SIARDDKModuleFactory(), new SQLServerJDBCModuleFactory()};

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

    final DatabaseImportModule importModule;
    final DatabaseExportModule exportModule;

    try {
      importModule = cli.getImportModule();
      exportModule = cli.getExportModule();
    } catch (ParseException e) {
      logger.error(e.getMessage(), e);
      cli.printHelp();
      logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
      return EXIT_CODE_COMMAND_PARSE_ERROR;
    } catch (LicenseNotAcceptedException e) {
      logger.error("The license must be accepted to use this module.");
      logger.error("==================================================");
      cli.printLicense(e.getLicense());
      logger.error("==================================================");
      logProgramFinish(EXIT_CODE_LICENSE_NOT_ACCEPTED);
      return EXIT_CODE_LICENSE_NOT_ACCEPTED;
    }

    int exitStatus = EXIT_CODE_GENERIC_ERROR;

    try {
      long startTime = System.currentTimeMillis();
      logger.info("Translating database: " + cli.getImportModuleName() + " to " + cli.getExportModuleName());
      importModule.getDatabase(exportModule);
      long duration = System.currentTimeMillis() - startTime;
      logger.info("Done in " + (duration / 60000) + "m " + (duration % 60000 / 1000) + "s");
      exitStatus = EXIT_CODE_OK;
    } catch (ModuleException e) {
      if (e.getCause() != null && e.getCause() instanceof ClassNotFoundException
        && "sun.jdbc.odbc.JdbcOdbcDriver".equals(e.getCause().getMessage())) {
        logger.error("Could not find the Java ODBC driver, "
          + "please run this program under Windows to use the JDBC-ODBC bridge.", e.getCause());
      } else if (e.getModuleErrors() != null) {
        for (Map.Entry<String, Throwable> entry : e.getModuleErrors().entrySet()) {
          logger.error(entry.getKey(), entry.getValue());
        }
      } else {
        logger.error("Error while importing/exporting", e);
      }
    } catch (UnknownTypeException e) {
      logger.error("Error while importing/exporting", e);
    } catch (InvalidDataException e) {
      logger.error("Error while importing/exporting", e);
    } catch (Exception e) {
      logger.error("Unexpected exception", e);
    }

    logProgramFinish(exitStatus);

    return exitStatus;
  }

  private static void logProgramStart() {
    logger.debug("#########################################################");
    logger.debug("#   START-ID-" + execID);
    logger.debug("#   START v" + APP_VERSION);
    logger.debug("#########################################################");
  }

  private static void logProgramFinish(int exitStatus) {
    logger.debug("#########################################################");
    logger.debug("#   FINISH-ID-" + execID);
    logger.debug("#   FINISH v" + APP_VERSION);
    logger.debug("#   EXIT CODE: " + exitStatus);
    logger.debug("#########################################################");
  }

  private static String getProgramVersion() {
    try {
      return Main.class.getPackage().getImplementationVersion();
    } catch (Exception e) {
      logger.debug("Problem getting program version", e);
      return null;
    }
  }
}
