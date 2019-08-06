/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles command line interface.
 * 
 * Uses lazy parsing of parameters. Which means that the parameters are parsed
 * implicitly when something is requested that required them to be processed
 * (example: get the specified import or export modules).
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CLI {
  private static final Logger LOGGER = LoggerFactory.getLogger(CLI.class);

  private final CLIMigrate migrate;
  private final CLIEdit edit;
  private final CLIValidate validate;
  private final CLIHelp help;

  private final List<String> commandLineArguments;

  private boolean recognizedCommand = false;

  /**
   * Create a new CLI handler
   *
   * @param commandLineArguments
   *          List of command line parameters as they are received by Main.main
   * @param databaseModuleFactories
   *          List of available module factories
   */
  public CLI(List<String> commandLineArguments, Collection<DatabaseModuleFactory> databaseModuleFactories,
             Collection<DatabaseFilterFactory> databaseFilterFactories, Collection<EditModuleFactory> editModuleFactories, Collection<ValidateModuleFactory> validateModuleFactories) {
    this.commandLineArguments = commandLineArguments;
    this.migrate = new CLIMigrate(commandLineArguments, databaseModuleFactories, databaseFilterFactories);
    this.edit = new CLIEdit(commandLineArguments, editModuleFactories);
    this.validate = new CLIValidate(commandLineArguments, validateModuleFactories);
    this.help = new CLIHelp(commandLineArguments);
    prepareCLIHelp(databaseModuleFactories, databaseFilterFactories, editModuleFactories, validateModuleFactories);
  }

  public CLIMigrate getCLIMigrate() {
    return this.migrate;
  }

  public CLIEdit getCLIEdit() {
    return this.edit;
  }

  public CLIValidate getCLIValidate() {
    return this.validate;
  }

  public CLIHelp getCLIHelp() {
    return this.help;
  }

  public void disableEncryption() {
    getCLIMigrate().disableEncryption();
  }

  /**
   * Outputs the usage text to STDOUT
   */
  public void printUsage() {
    getCLIHelp().printUsage(System.out);
  }

  private void prepareCLIHelp(Collection<DatabaseModuleFactory> databaseModuleFactories,
                              Collection<DatabaseFilterFactory> databaseFilterFactories, Collection<EditModuleFactory> editModuleFactories, Collection<ValidateModuleFactory> validateModuleFactories) {
    getCLIHelp().setDatabaseModuleFactory(databaseModuleFactories);
    getCLIHelp().setDatabaseFilterFactory(databaseFilterFactories);
    getCLIHelp().setEditModuleFactories(editModuleFactories);
    getCLIHelp().setValidateModuleFactories(validateModuleFactories);
  }

  /**
   * Checks if the CLI command is GUI
   * 
   * @return true if GUI is needed otherwise false
   */
  public boolean isGUI() {
    if (commandLineArguments.isEmpty()) {
      recognizedCommand = true;
      return true;
    }
    return false;
  }

  /**
   * Checks if the CLI command is help
   * 
   * @return true if help is needed otherwise false
   */
  public boolean isHelp() {
    if (commandLineArguments.isEmpty()) {
      return false;
    }

    String arg = commandLineArguments.get(0);
    boolean value = Constants.DBPTK_OPTION_HELP.equalsIgnoreCase(arg)
      || Constants.DBPTK_OPTION_HELP_SMALL.equalsIgnoreCase(arg);
    if (value) {
      recognizedCommand = true;
      return true;
    }

    return false;
  }

  /**
   * Checks if the CLI command is migrate
   * 
   * @return true if migration is needed otherwise false
   */
  public boolean isMigration() {
    if (commandLineArguments.isEmpty()) {
      return false;
    }
    String arg = commandLineArguments.get(0);
    boolean value = Constants.DBPTK_OPTION_MIGRATE.equalsIgnoreCase(arg);

    if (value) {
      recognizedCommand = true;
      return true;
    }

    return false;
  }

  /**
   * Checks if the CLI command is edit
   * 
   * @return true if edition is needed otherwise false
   */
  public boolean isEdition() {
    if (commandLineArguments.isEmpty()) {
      return false;
    }
    String arg = commandLineArguments.get(0);
    boolean value = Constants.DBPTK_OPTION_EDIT.equalsIgnoreCase(arg);

    if (value) {
      recognizedCommand = true;
      return true;
    }

    return false;
  }

  /**
   * Checks if the CLI command is validate
   *
   * @return true if validation is needed otherwise false
   */
  public boolean isValidation() {
    if (commandLineArguments.isEmpty()) {
      return false;
    }
    String arg = commandLineArguments.get(0);
    boolean value = Constants.DBPTK_OPTION_VALIDATE.equalsIgnoreCase(arg);

    if (value) {
      recognizedCommand = true;
      return true;
    }

    return false;
  }

  /**
   * Logs operating system information
   */
  public void logOperatingSystemInfo() {
    for (Map.Entry<String, String> entry : getOperatingSystemInfo().entrySet()) {
      LOGGER.debug(entry.getKey() + ": " + entry.getValue());
    }
  }

  /**
   * Returns the command argument
   *
   * @return the command argument
   */
  public String getArgCommand() {
    return this.commandLineArguments.get(0);
  }

  /**
   * Checks if any command was recognized by the CLI
   *
   * @return true if succeed otherwise returns false
   */
  public boolean getRecognizedCommand() {
    return this.recognizedCommand;
  }

  public boolean usingUTF8() {
    return Charset.defaultCharset().equals(Charset.forName("UTF-8"));
  }

  /**
   * Get operating system information.
   *
   * @return An order-preserving map which keys are a description (String) of the
   *         information contained in the values (which are also of type String).
   */
  private HashMap<String, String> getOperatingSystemInfo() {
    LinkedHashMap<String, String> result = new LinkedHashMap<>();

    result.put("Operating system", System.getProperty("os.name", "unknown"));
    result.put("Architecture", System.getProperty("os.arch", "unknown"));
    result.put("Version", System.getProperty("os.version", "unknown"));
    result.put("Java vendor", System.getProperty("java.vendor", "unknown"));
    result.put("Java version", System.getProperty("java.version", "unknown"));
    result.put("Java class version", System.getProperty("java.class.version", "unknown"));
    // Charset.defaultCharset() is bugged on java version 5 and fixed on java 6
    result.put("Default Charset reported by java", Charset.defaultCharset().toString());
    result.put("Default Charset used by StreamWriter", getDefaultCharSet());
    result.put("file.encoding property", System.getProperty("file.encoding"));

    return result;
  }

  private static String getDefaultCharSet() {
    OutputStreamWriter dummyWriter = new OutputStreamWriter(new ByteArrayOutputStream());
    String encoding = dummyWriter.getEncoding();
    return encoding;
  }
}
