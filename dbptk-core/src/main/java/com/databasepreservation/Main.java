package com.databasepreservation;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.jdbc.JDBCModuleFactory;
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

  public static final String APP_NAME = "db-preservation-toolkit - KEEP SOLUTIONS";

  public static final String NAME = "db-preservation-toolkit";

  private static final Logger logger = Logger.getLogger(Main.class);

  /**
   * @param args
   *          the console arguments
   */
  public static void main(String... args) {
    System.exit(internal_main(args));
  }

  public static int internal_main(String... args) {
    final DatabaseImportModule importModule;
    final DatabaseExportModule exportModule;

    CLI cli = new CLI(Arrays.asList(args), new JDBCModuleFactory(), new MySQLModuleFactory(),
      new Oracle12cModuleFactory(), new PostgreSQLModuleFactory(), new SIARD1ModuleFactory(),
      new SIARD2ModuleFactory(), new SIARDDKModuleFactory(),
      new SQLServerJDBCModuleFactory());
    try {
      importModule = cli.getImportModule();
      exportModule = cli.getExportModule();
    } catch (ParseException e) {
      System.err.println("Error: " + e.getMessage() + "\n");
      cli.printHelp();
      return EXIT_CODE_COMMAND_PARSE_ERROR;
    } catch (LicenseNotAcceptedException e) {
      System.err.println("Error: The license must be accepted to use this module.");
      System.err.println("==================================================");
      cli.printLicense(e.getLicense());
      System.err.println("==================================================");
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
        && e.getCause().getMessage().equals("sun.jdbc.odbc.JdbcOdbcDriver")) {
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

    return exitStatus;
  }
}
