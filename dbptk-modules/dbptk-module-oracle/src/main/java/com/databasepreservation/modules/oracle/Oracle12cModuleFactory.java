/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.oracle.in.Oracle12cJDBCImportModule;
import com.databasepreservation.modules.oracle.out.Oracle12cJDBCExportModule;
import org.apache.commons.lang3.StringUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Oracle12cModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_SERVER_NAME = "server-name";
  public static final String PARAMETER_PORT_NUMBER = "port-number";
  public static final String PARAMETER_INSTANCE = "instance";
  public static final String PARAMETER_USERNAME = "username";
  public static final String PARAMETER_PASSWORD = "password";
  public static final String PARAMETER_SOURCE_SCHEMA = "source-schema";
  public static final String PARAMETER_ACCEPT_LICENSE = "accept-license";
  public static final String PARAMETER_CUSTOM_VIEWS = "custom-views";

  private static final String licenseURL = "http://www.oracle.com/technetwork/licenses/distribution-license-152002.html";

  private static final Parameter serverName = new Parameter().shortName("s").longName(PARAMETER_SERVER_NAME)
    .description("the name (or IP address) of the Oracle server").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the server port number").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter instance = new Parameter().shortName("i").longName(PARAMETER_INSTANCE)
    .description("the name of the instance to use in the connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter username = new Parameter().shortName("u").longName(PARAMETER_USERNAME)
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter sourceSchema = new Parameter().shortName("sc").longName(PARAMETER_SOURCE_SCHEMA)
    .hasArgument(true).setOptionalArgument(false).required(false).description(
      "name of the specific schema (from import) that should be exported to oracle (default: the first schema is exported)");

  private static final Parameter acceptLicense = new Parameter().shortName("al").longName(PARAMETER_ACCEPT_LICENSE)
    .description("declare that you accept OTN License Agreement, which is necessary to use this module")
    .hasArgument(false).valueIfSet("true").valueIfNotSet("false").required(false);

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
    return "oracle";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(serverName.longName(), serverName);
    parameterHashMap.put(instance.longName(), instance);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(acceptLicense.longName(), acceptLicense);
    parameterHashMap.put(sourceSchema.longName(), sourceSchema);
    parameterHashMap.put(customViews.longName(), customViews);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(serverName, instance, username, password, portNumber, acceptLicense, customViews), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(
      Arrays.asList(serverName, instance, username, password, portNumber, acceptLicense, sourceSchema), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
          throws ModuleException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(instance);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException().withLicenseInfo(getLicenseText("--import-" + acceptLicense.longName()));
    }

    Path pCustomViews = null;
    if (StringUtils.isNotBlank(parameters.get(customViews))) {
      pCustomViews = Paths.get(parameters.get(customViews));
    }

    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE, pDatabase,
      PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
      pPortNumber.toString());
    return new Oracle12cJDBCImportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword, pCustomViews);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(instance);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);
    String pSourceSchema = parameters.get(sourceSchema);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException().withLicenseInfo(getLicenseText("--export-" + acceptLicense.longName()));
    }

    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    reporter.exportModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE, pDatabase,
      PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
      pPortNumber.toString(), PARAMETER_SOURCE_SCHEMA, pSourceSchema);
    return new Oracle12cJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword, pSourceSchema);
  }

  private String getLicenseText(String parameter) {
    return "Please agree to the Oracle Technology Network Development and Distribution License Terms before using this module.\n"
      + "The Oracle Technology Network Development and Distribution License Terms are available at\n" + licenseURL
      + "\nTo agree you must specify the additional parameter " + parameter + " in your command.";
  }
}
