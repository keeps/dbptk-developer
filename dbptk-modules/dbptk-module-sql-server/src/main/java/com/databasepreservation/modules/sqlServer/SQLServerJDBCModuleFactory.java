/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sqlServer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.exception.ModuleException;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLServerJDBCModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_SERVER_NAME = "server-name";
  public static final String PARAMETER_DATABASE = "database";
  public static final String PARAMETER_USERNAME = "username";
  public static final String PARAMETER_PASSWORD = "password";
  public static final String PARAMETER_USE_INTEGRATED_LOGIN = "use-integrated-login";
  public static final String PARAMETER_DISABLE_ENCRYPTION = "disable-encryption";
  public static final String PARAMETER_INSTANCE_NAME = "instance-name";
  public static final String PARAMETER_PORT_NUMBER = "port-number";
  public static final String PARAMETER_CUSTOM_VIEWS = "custom-views";

  private static final Parameter serverName = new Parameter().shortName("s").longName(PARAMETER_SERVER_NAME)
    .description("the name (host name) of the server").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter database = new Parameter().shortName("db").longName(PARAMETER_DATABASE)
    .description("the name of the database we'll be accessing").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter username = new Parameter().shortName("u").longName(PARAMETER_USERNAME)
    .description("the name of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
    .description("the password of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter useIntegratedLogin = new Parameter().shortName("l")
    .longName(PARAMETER_USE_INTEGRATED_LOGIN).description("use windows login; by default the SQL Server login is used")
    .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true");

  private static final Parameter disableEncryption = new Parameter().shortName("de")
    .longName(PARAMETER_DISABLE_ENCRYPTION).description("use to turn off encryption in the connection")
    .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true");

  private static final Parameter instanceName = new Parameter().shortName("in").longName(PARAMETER_INSTANCE_NAME)
    .description("the name of the instance").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the server port number").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter customViews = new Parameter().shortName("cv").longName(PARAMETER_CUSTOM_VIEWS)
          .description("the path to a custom view query list file").hasArgument(true).setOptionalArgument(false)
          .required(false);

  private static final ParameterGroup instanceName_portNumber = new ParameterGroup(false, instanceName, portNumber);

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
    return "microsoft-sql-server";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(serverName.longName(), serverName);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(useIntegratedLogin.longName(), useIntegratedLogin);
    parameterHashMap.put(disableEncryption.longName(), disableEncryption);
    parameterHashMap.put(instanceName.longName(), instanceName);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(customViews.longName(), customViews);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(
      Arrays.asList(serverName, database, username, password, useIntegratedLogin, disableEncryption, customViews),
      Arrays.asList(instanceName_portNumber));
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(
      Arrays.asList(serverName, database, username, password, useIntegratedLogin, disableEncryption),
      Arrays.asList(instanceName_portNumber));
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
          throws ModuleException {
    // String values
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pUseIntegratedLogin = Boolean.parseBoolean(parameters.get(useIntegratedLogin));
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    }
    String pInstanceName = null;
    if (StringUtils.isNotBlank(parameters.get(instanceName))) {
      pInstanceName = parameters.get(instanceName);
    }

    Path pCustomViews = null;
    if (StringUtils.isNotBlank(parameters.get(customViews))) {
      pCustomViews = Paths.get(parameters.get(customViews));
    }

    if (pPortNumber != null) {
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin), PARAMETER_PORT_NUMBER,
        pPortNumber.toString());
      return new SQLServerJDBCImportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword,
        pUseIntegratedLogin, pEncrypt, pCustomViews);
    } else if (pInstanceName != null) {
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin), PARAMETER_INSTANCE_NAME, pInstanceName);
      return new SQLServerJDBCImportModule(pServerName, pInstanceName, pDatabase, pUsername, pPassword,
        pUseIntegratedLogin, pEncrypt, pCustomViews);
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin));
      return new SQLServerJDBCImportModule(pServerName, pDatabase, pUsername, pPassword, pUseIntegratedLogin, pEncrypt, pCustomViews);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    // String values
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pUseIntegratedLogin = Boolean.parseBoolean(parameters.get(useIntegratedLogin));
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    }
    String pInstanceName = null;
    if (StringUtils.isNotBlank(parameters.get(instanceName))) {
      pInstanceName = parameters.get(instanceName);
    }

    if (pPortNumber != null) {
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin), PARAMETER_PORT_NUMBER,
        pPortNumber.toString());
      return new SQLServerJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword,
        pUseIntegratedLogin, pEncrypt);
    } else if (pInstanceName != null) {
      reporter.exportModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin), PARAMETER_INSTANCE_NAME, pInstanceName);
      return new SQLServerJDBCExportModule(pServerName, pInstanceName, pDatabase, pUsername, pPassword,
        pUseIntegratedLogin, pEncrypt);
    } else {
      reporter.exportModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_DATABASE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED,
        PARAMETER_USE_INTEGRATED_LOGIN, String.valueOf(pUseIntegratedLogin));
      return new SQLServerJDBCExportModule(pServerName, pDatabase, pUsername, pPassword, pUseIntegratedLogin, pEncrypt);
    }
  }
}
