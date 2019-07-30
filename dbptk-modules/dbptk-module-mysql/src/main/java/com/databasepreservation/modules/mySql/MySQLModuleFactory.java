/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.mySql;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.mySql.in.MySQLJDBCImportModule;
import com.databasepreservation.modules.mySql.out.MySQLJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySQLModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_HOSTNAME = "hostname";
  public static final String PARAMETER_PORT_NUMBER = "port-number";
  public static final String PARAMETER_DATABASE = "database";
  public static final String PARAMETER_USERNAME = "username";
  public static final String PARAMETER_PASSWORD = "password";
  public static final String PARAMETER_DISABLE_ENCRYPTION = "disable-encryption";
  public static final String PARAMETER_CUSTOM_VIEWS = "custom-views";

  private static final Parameter hostname = new Parameter().shortName("h").longName(PARAMETER_HOSTNAME)
    .description("the hostname of the MySQL server").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the port that the MySQL server is listening").hasArgument(true).setOptionalArgument(false)
    .required(false).valueIfNotSet("3306");

  private static final Parameter database = new Parameter().shortName("db").longName(PARAMETER_DATABASE)
    .description("the name of the MySQL database").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter username = new Parameter().shortName("u").longName(PARAMETER_USERNAME)
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter disableEncryption = new Parameter().shortName("de")
    .longName(PARAMETER_DISABLE_ENCRYPTION).description("use to turn off encryption in the connection")
    .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true");

  private static final Parameter customViews = new Parameter().shortName("cv").longName(PARAMETER_CUSTOM_VIEWS)
    .description("the path to a custom view query list file").hasArgument(true).setOptionalArgument(false)
    .required(false);

  @Override
  public boolean producesImportModules() {
    return true;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "mysql";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(disableEncryption.longName(), disableEncryption);
    parameterHashMap.put(customViews.longName(), customViews);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(Arrays.asList(hostname.inputType(INPUT_TYPE.TEXT), portNumber.inputType(INPUT_TYPE.NUMBER),
      username.inputType(INPUT_TYPE.TEXT), password.inputType(INPUT_TYPE.PASSWORD),
      database.inputType(INPUT_TYPE.TEXT), disableEncryption.inputType(INPUT_TYPE.CHECKBOX)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(
      Arrays.asList(hostname, portNumber, database, username, password, disableEncryption, customViews), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, portNumber, database, username, password, disableEncryption), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
          throws ModuleException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    int pPortNumber;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    Path pCustomViews = null;
    if (StringUtils.isNotBlank(parameters.get(customViews))) {
      pCustomViews = Paths.get(parameters.get(customViews));
    }

    if (pCustomViews == null) {
      reporter.importModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
        PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt));
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
        PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt), PARAMETER_CUSTOM_VIEWS,
        pCustomViews.toString());
    }

    return new MySQLJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt, pCustomViews);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    int pPortNumber;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    reporter.exportModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
      PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
      Integer.toString(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt));

    return new MySQLJDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt);
  }
}
