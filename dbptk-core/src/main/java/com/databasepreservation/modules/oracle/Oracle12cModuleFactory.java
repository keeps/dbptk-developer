package com.databasepreservation.modules.oracle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.oracle.in.Oracle12cJDBCImportModule;
import com.databasepreservation.modules.oracle.out.Oracle12cJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Oracle12cModuleFactory implements DatabaseModuleFactory {
  private static final String licenseURL = "http://www.oracle.com/technetwork/licenses/distribution-license-152002.html";

  private static final Parameter serverName = new Parameter().shortName("s").longName("server-name")
    .description("the name (or IP address) of the Oracle server").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName("port-number")
    .description("the server port number").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter database = new Parameter().shortName("db").longName("database")
    .description("the name of the database to use in the connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter username = new Parameter().shortName("u").longName("username")
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName("password")
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter sourceSchema = new Parameter()
    .shortName("sc")
    .longName("source-schema")
    .hasArgument(true)
    .setOptionalArgument(false)
    .required(false)
    .description(
      "the name of the source schema to export to the Oracle database. A schema with this name must exist in"
        + " the Oracle database and it must be the default tablespace for the specified user. If omitted, the name of"
        + " the first schema will be used");

  private static final Parameter acceptLicense = new Parameter().shortName("al").longName("accept-license")
    .description("declare that you accept OTN License Agreement, which is necessary to use this module")
    .hasArgument(false).valueIfSet("true").valueIfNotSet("false").required(false);

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
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(serverName.longName(), serverName);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(acceptLicense.longName(), acceptLicense);
    parameterHashMap.put(sourceSchema.longName(), sourceSchema);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(serverName, database, username, password, portNumber, acceptLicense), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(serverName, database, username, password, portNumber, acceptLicense,
      sourceSchema), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException(getLicenseText("--import-" + acceptLicense.longName()));
    }

    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    Reporter.importModuleParameters(getModuleName(), "server name", pServerName, "database", pDatabase, "username",
      pUsername, "password", Reporter.MESSAGE_FILTERED, "port number", pPortNumber.toString());
    return new Oracle12cJDBCImportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);
    String pSourceSchema = parameters.get(sourceSchema);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException(getLicenseText("--export-" + acceptLicense.longName()));
    }

    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    Reporter.exportModuleParameters(getModuleName(), "server name", pServerName, "database", pDatabase, "username",
      pUsername, "password", Reporter.MESSAGE_FILTERED, "port number", pPortNumber.toString(),
      "source schema", pSourceSchema);
    return new Oracle12cJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword, pSourceSchema);
  }

  private String getLicenseText(String parameter) {
    return "Please agree to the Oracle Technology Network Development and Distribution License Terms before using this module.\n"
      + "The Oracle Technology Network Development and Distribution License Terms are available at\n"
      + licenseURL
      + "\nTo agree you must specify the additional parameter " + parameter + " in your command.";
  }
}
