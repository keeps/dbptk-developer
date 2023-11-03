/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.cli.CLI;
import com.databasepreservation.cli.CLIEdit;
import com.databasepreservation.cli.CLIHelp;
import com.databasepreservation.cli.CLIMigrate;
import com.databasepreservation.cli.CLIValidate;
import com.databasepreservation.model.exception.EditDatabaseMetadataParserException;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.RequiredParameterException;
import com.databasepreservation.model.exception.SIARDVersionNotSupportedException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.modules.filters.ObservableFilter;
import com.databasepreservation.model.modules.filters.ProgressLoggerObserver;
import com.databasepreservation.model.modules.validate.ProgressValidationLoggerObserver;
import com.databasepreservation.model.reporters.NoOpReporter;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.utils.ConfigUtils;
import com.databasepreservation.utils.MiscUtils;
import com.databasepreservation.utils.ReflectionUtils;

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
  public static final int EXIT_CODE_FILE_NOT_FOUND = 6;
  public static final int EXIT_CODE_REQUIRED_PARAMETER_MISSING = 7;

  private static final String execID = UUID.randomUUID().toString();

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static Reporter reporter = null;

  private static Reporter getReporter() {
    if (reporter == null) {
      reporter = new Reporter();
    }
    return reporter;
  }

  /**
   * @param args
   *          the console arguments
   */
  public static void main(String[] args) {
    CLI cli = new CLI(Arrays.asList(args),
            ReflectionUtils.collectDatabaseModuleFactories(),
            ReflectionUtils.collectDatabaseFilterFactory(),
            ReflectionUtils.collectEditModuleFactories(),
            ReflectionUtils.collectValidateModuleFactories());
            System.exit(internalMain(cli));
  }

  public static int internalMainUsedOnlyByTestClasses(String... args) {
    // start by setting reporter to NoOpReporter, since during whole-application
    // testing we should not create reports
    if (reporter == null) {
      reporter = new NoOpReporter();
    }
    CLI cli = new CLI(Arrays.asList(args), ReflectionUtils.collectDatabaseModuleFactories(),
      ReflectionUtils.collectDatabaseFilterFactory(), ReflectionUtils.collectEditModuleFactories(),
      ReflectionUtils.collectValidateModuleFactories());
    return internalMain(cli);
  }

  public static int internalMain(CLI cli) {
    logProgramStart();
    cli.logOperatingSystemInfo();

    int exitStatus = EXIT_CODE_OK;

    // avoid SAX processing limit of 50 million elements
    System.setProperty("totalEntitySizeLimit", "0");
    System.setProperty("jdk.xml.totalEntitySizeLimit", "0");

    boolean isGUI = cli.isGUI();
    boolean isHelp = cli.isHelp();
    boolean isMigrate = cli.isMigration();
    boolean isEdit = cli.isEdition();
    boolean isValidation = cli.isValidation();

    if (!cli.getRecognizedCommand()) {
      LOGGER.error("Command '{}' not a valid command.", cli.getArgCommand());
      cli.printUsage();
      exitStatus = EXIT_CODE_GENERIC_ERROR;
    } else {
      if (cli.usingUTF8()) {
        if (isGUI) {
          exitStatus = runHelp(cli.getCLIHelp());
        } else {
          if (isHelp) {
            exitStatus = runHelp(cli.getCLIHelp());
          } else {
            if (isMigrate) {
              exitStatus = runMigration(cli.getCLIMigrate());
            } else if (isEdit) {
              exitStatus = runEdition(cli.getCLIEdit(), cli.getCLIHelp());
            } else if (isValidation) {
              exitStatus = runValidation(cli.getCLIValidate(), cli.getCLIHelp());
            }

            if (exitStatus == EXIT_CODE_CONNECTION_ERROR) {
              LOGGER.info("Disabling connection encryption (for modules that support it) and trying again.");
              cli.disableEncryption();
              exitStatus = runMigration(cli.getCLIMigrate());
            }
          }
        }
      } else {
        exitStatus = EXIT_CODE_NOT_USING_UTF8;
        LOGGER.error("The charset in use is not UTF-8.");
        LOGGER.info("Please try forcing UTF-8 charset by running the application with:");
        LOGGER.info("   java \"-Dfile.encoding=UTF-8\" -jar ...");
      }
    }

    try {
      getReporter().close();
    } catch (IOException e) {
      LOGGER.debug("There was a problem closing the report file.", e);
    }

    LOGGER.info("Log files and migration reports were saved in {}", ConfigUtils.getHomeDirectory());
    LOGGER.info("Troubleshooting information can be found at https://github.com/keeps/dbptk-developer/wiki/Troubleshooting");
    LOGGER.info("Please report any problems at https://github.com/keeps/dbptk-developer/issues/new");
    logProgramFinish(exitStatus);

    return exitStatus;
  }

  private static int runHelp(CLIHelp cli) {
    cli.printHelp(System.out);
    return EXIT_CODE_OK;
  }

  private static int runValidation(CLIValidate cli, CLIHelp help) {
    cli.removeCommand(); // removes the validate argument

    SIARDValidation siardValidation;
    int exitStatus = EXIT_CODE_GENERIC_ERROR;

    if (cli.emptyArguments()) {
      help.printValidationUsage(System.out);
      exitStatus = EXIT_CODE_OK;
    } else {
      try {
        siardValidation = SIARDValidation.newInstance().validateModule(cli.getValidateModuleFactory())
          .validateModuleParameters(cli.getValidateModuleParameters()).reporter(getReporter());
      } catch (ParseException e) {
        LOGGER.error(e.getMessage(), e);
        logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
        return EXIT_CODE_COMMAND_PARSE_ERROR;
      }

      siardValidation = siardValidation.observer(new ProgressValidationLoggerObserver());

      try {
        long startTime = System.currentTimeMillis();
        long duration;
        LOGGER.info("Validate SIARD at '{}'", cli.getSIARDPackage());
        siardValidation.validate();
        duration = System.currentTimeMillis() - startTime;
        exitStatus = EXIT_CODE_OK;
        LOGGER.info("Validate SIARD took {}m {}s to complete.", duration / 60000, duration % 60000 / 1000);
      } catch (SiardNotFoundException e) {
        LOGGER.error("{}: {}", e.getMessage(), e.getPath());
        return EXIT_CODE_FILE_NOT_FOUND;
      } catch (SIARDVersionNotSupportedException e) {
        LOGGER.error("{}: {}", e.getMessage(), e.getVersionInfo());
      } catch (ModuleException e) {
        LOGGER.error(e.getMessage(), e);
      } catch (Exception e) {
        LOGGER.error("Fatal error: Unexpected exception (" + e.getMessage() + ")", e);
      }
    }

    return exitStatus;
  }

  private static int runEdition(CLIEdit cli, CLIHelp help) {
    cli.removeCommand(); // removes the edit argument

    SIARDEdition siardEdition;
    int exitStatus = EXIT_CODE_GENERIC_ERROR;

    if (cli.emptyArguments()) {
      help.printEditUsage(System.out);
      exitStatus = EXIT_CODE_OK;
    } else {
      try {
        siardEdition = SIARDEdition.newInstance().editModule(cli.getEditModuleFactory())
          .editModuleParameters(cli.getEditModuleParameters()).reporter(getReporter());
      } catch (ParseException e) {
        LOGGER.error(e.getMessage(), e);
        logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
        return EXIT_CODE_COMMAND_PARSE_ERROR;
      }

      try {
        long startTime = System.currentTimeMillis();
        long duration;
        switch (cli.option()) {
          case CLIEdit.NO_OPTION:
          case CLIEdit.LIST_OPTION:
            LOGGER.info("Listing SIARD metadata of {}", cli.getSIARDPackage());
            siardEdition.list();
            duration = System.currentTimeMillis() - startTime;
            LOGGER.info("Listing SIARD metadata took {}m {}s to complete.", duration / 60000, duration % 60000 / 1000);
            break;
          case CLIEdit.SET_OPTION:
            LOGGER.info("Edit SIARD metadata of {}", cli.getSIARDPackage());
            siardEdition.edit();
            duration = System.currentTimeMillis() - startTime;
            LOGGER.info("Edit SIARD metadata took {}m {}s to complete.", duration / 60000, duration % 60000 / 1000);
            break;
          default:
        }

        exitStatus = EXIT_CODE_OK;
      } catch (EditDatabaseMetadataParserException e) {
        LOGGER.error(e.getMessage() + " on: " + e.getFaultyArgument(), e);

        return EXIT_CODE_COMMAND_PARSE_ERROR;
      } catch (SiardNotFoundException e) {
        LOGGER.error("{}: {}", e.getMessage(), e.getPath());
        return EXIT_CODE_FILE_NOT_FOUND;
      } catch (ModuleException e) {
        if (e.getCause().getClass().equals(NullPointerException.class))
          LOGGER.error(e.getMessage());
        if (!e.getClass().equals(ModuleException.class)) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    }

    return exitStatus;
  }

  private static int runMigration(CLIMigrate cli) {
    cli.removeCommand();
    DatabaseMigration databaseMigration;

    // obtain parameters and module factories, failing early if the command line
    // parameters are invalid
    try {
      databaseMigration = DatabaseMigration.newInstance().importModule(cli.getImportModuleFactory())
        .exportModule(cli.getExportModuleFactory()).importModuleParameters(cli.getImportModuleParameters())
        .exportModuleParameters(cli.getExportModuleParameters()).filterFactories(cli.getFilterFactories())
        .filterParameters(cli.getFilterParameters()).reporter(getReporter());
    } catch (ParseException e) {
      LOGGER.error(e.getMessage(), e);
      logProgramFinish(EXIT_CODE_COMMAND_PARSE_ERROR);
      return EXIT_CODE_COMMAND_PARSE_ERROR;
    }

    // adds a default filter, which for now just does progress logging
    databaseMigration.filter(new ObservableFilter(new ProgressLoggerObserver()));

    // converts the database using the specified modules, module parameters, and
    // filters
    int exitStatus = EXIT_CODE_GENERIC_ERROR;
    try {
      long startTime = System.currentTimeMillis();
      LOGGER.info("Converting database: {} to {}", cli.getImportModuleName(), cli.getExportModuleName());
      databaseMigration.migrate();
      long duration = System.currentTimeMillis() - startTime;
      LOGGER.info("Database migration took {}m {}s to complete.", duration / 60000, duration % 60000 / 1000);
      exitStatus = EXIT_CODE_OK;
    } catch (SiardNotFoundException e) {
      LOGGER.error("{}: {}", e.getMessage(), e.getPath());
      return EXIT_CODE_FILE_NOT_FOUND;
    } catch (LicenseNotAcceptedException e) {
      LOGGER.trace("LicenseNotAcceptedException", e);
      LOGGER.info("The license must be accepted to use this module.");
      LOGGER.info("==================================================");
      LOGGER.info(e.getLicenseInfo());
      LOGGER.info("==================================================");
      logProgramFinish(EXIT_CODE_LICENSE_NOT_ACCEPTED);
      return EXIT_CODE_LICENSE_NOT_ACCEPTED;
    } catch (RequiredParameterException e) {
      LOGGER.error("Parameter {} is required", e.getParameter());
      return EXIT_CODE_REQUIRED_PARAMETER_MISSING;
    } catch (ModuleException e) {
      if (!e.getClass().equals(ModuleException.class)) {
        LOGGER.error(e.getMessage(), e);
      } else {
        if ("SQL error while connecting".equalsIgnoreCase(e.getMessage())) {
          LOGGER.error("Connection error while importing/exporting", e);
          exitStatus = EXIT_CODE_CONNECTION_ERROR;
        }

        if (e.getExceptionMap() != null) {
          for (Map.Entry<String, Throwable> entry : e.getExceptionMap().entrySet()) {
            LOGGER.error(entry.getKey(), entry.getValue());
          }
        } else {
          LOGGER.error("Fatal error while converting the database (" + e.getMessage() + ")", e);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Fatal error: Unexpected exception (" + e.getMessage() + ")", e);
    }
    return exitStatus;
  }

  private static void logProgramStart() {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   START-ID-{}", execID);
    LOGGER.debug("#   START v{}", MiscUtils.APP_VERSION);
    LOGGER.debug("#   version info: {}", ConfigUtils.getVersionInfo());
    LOGGER.debug("#########################################################");
  }

  private static void logProgramFinish(int exitStatus) {
    LOGGER.debug("#########################################################");
    LOGGER.debug("#   FINISH-ID-{}", execID);
    LOGGER.debug("#   FINISH v{}", MiscUtils.APP_VERSION);
    LOGGER.debug("#   EXIT CODE: {}", exitStatus);
    LOGGER.debug("#########################################################");
  }
}
