/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.oracle;

import static com.databasepreservation.model.Reporter.MESSAGE_FILTERED;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.RemoteConnectionManager;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.oracle.in.Oracle12cJDBCImportModule;
import com.databasepreservation.modules.oracle.out.Oracle12cJDBCExportModule;
import com.databasepreservation.utils.ModuleConfigurationUtils;

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
  public static final String PARAMETER_SSH = "ssh";
  public static final String PARAMETER_SSH_HOST = "ssh-host";
  public static final String PARAMETER_SSH_USER = "ssh-user";
  public static final String PARAMETER_SSH_PASSWORD = "ssh-password";
  public static final String PARAMETER_SSH_PORT = "ssh-port";

  private static final String LICENSE_URL = "http://www.oracle.com/technetwork/licenses/distribution-license-152002.html";

  private static final Parameter serverName = new Parameter().shortName("s").longName(PARAMETER_SERVER_NAME)
    .description("the name (or IP address) of the Oracle server").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the server port number").valueIfNotSet("1521").hasArgument(true).setOptionalArgument(false)
    .required(true);

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

  private static final Parameter ssh = new Parameter().shortName("ssh").longName(PARAMETER_SSH)
    .description("use to perform a SSH remote connection").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  private static final Parameter sshHost = new Parameter().shortName("sh").longName(PARAMETER_SSH_HOST)
    .description("the hostname of the remote server").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter sshUser = new Parameter().shortName("su").longName(PARAMETER_SSH_USER)
    .description("the name of the remote user to use in the SSH connection").hasArgument(true)
    .setOptionalArgument(false).required(false);

  private static final Parameter sshPassword = new Parameter().shortName("spw").longName(PARAMETER_SSH_PASSWORD)
    .description("the password of the remote user to use in the SSH connection").hasArgument(true)
    .setOptionalArgument(false).required(false);

  private static final Parameter sshPort = new Parameter().shortName("spn").longName(PARAMETER_SSH_PORT)
    .description("the port number remote server is listening").hasArgument(true).setOptionalArgument(false)
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
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(serverName.longName(), serverName);
    parameterHashMap.put(instance.longName(), instance);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(acceptLicense.longName(), acceptLicense);
    parameterHashMap.put(sourceSchema.longName(), sourceSchema);
    parameterHashMap.put(ssh.longName(), ssh);
    parameterHashMap.put(sshHost.longName(), sshHost);
    parameterHashMap.put(sshUser.longName(), sshUser);
    parameterHashMap.put(sshPassword.longName(), sshPassword);
    parameterHashMap.put(sshPort.longName(), sshPort);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(Arrays.asList(serverName.inputType(INPUT_TYPE.TEXT), portNumber.inputType(INPUT_TYPE.NUMBER),
      username.inputType(INPUT_TYPE.TEXT), password.inputType(INPUT_TYPE.PASSWORD), instance.inputType(INPUT_TYPE.TEXT),
      acceptLicense.inputType(INPUT_TYPE.CHECKBOX)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(serverName, instance, username, password, portNumber, acceptLicense, ssh,
      sshHost, sshUser, sshPassword, sshPort), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(serverName, instance, username, password, portNumber, acceptLicense,
      sourceSchema, ssh, sshHost, sshUser, sshPassword, sshPort), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    return buildImportModule(parameters, ModuleConfigurationUtils.getDefaultModuleConfiguration(), reporter);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters,
    ModuleConfiguration moduleConfiguration, Reporter reporter) throws ModuleException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(instance);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException().withLicenseInfo(getLicenseText("--import-" + acceptLicense.longName()));
    }

    // boolean
    boolean pSSH = Boolean.parseBoolean(parameters.get(ssh));
    final String pSSHHost = parameters.get(sshHost);
    final String pSSHUser = parameters.get(sshUser);
    final String pSSHPassword = parameters.get(sshPassword);

    String pSSHPortNumber = "22";
    if (StringUtils.isNotBlank(parameters.get(sshPort))) {
      pSSHPortNumber = parameters.get(sshPort);
    }

    // optional
    int pPortNumber;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    if (pSSH) {
      RemoteConnectionManager.getInstance().setup(pSSHHost, pSSHUser, pSSHPassword, pSSHPortNumber);
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber), PARAMETER_SSH_HOST, pSSHHost, PARAMETER_SSH_USER, pSSHUser,
        PARAMETER_SSH_PASSWORD, MESSAGE_FILTERED, PARAMETER_SSH_PORT, pSSHPortNumber);
      return new Oracle12cJDBCImportModule(moduleConfiguration, getModuleName(), pServerName, pPortNumber, pDatabase,
        pUsername, pPassword, pSSHHost, pSSHUser, pSSHPassword, pSSHPortNumber);
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber));
      return new Oracle12cJDBCImportModule(moduleConfiguration, getModuleName(), pServerName, pPortNumber, pDatabase,
        pUsername, pPassword);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    String pServerName = parameters.get(serverName);
    String pDatabase = parameters.get(instance);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);
    String pSourceSchema = parameters.get(sourceSchema);

    boolean pAcceptLicense = Boolean.parseBoolean(parameters.get(acceptLicense));

    if (!pAcceptLicense) {
      throw new LicenseNotAcceptedException().withLicenseInfo(getLicenseText("--export-" + acceptLicense.longName()));
    }

    // boolean
    boolean pSSH = Boolean.parseBoolean(parameters.get(ssh));
    final String pSSHHost = parameters.get(sshHost);
    final String pSSHUser = parameters.get(sshUser);
    final String pSSHPassword = parameters.get(sshPassword);

    String pSSHPortNumber = "22";
    if (StringUtils.isNotBlank(parameters.get(sshPort))) {
      pSSHPortNumber = parameters.get(sshPort);
    }

    int pPortNumber = Integer.parseInt(parameters.get(portNumber));

    if (pSSH) {
      reporter.exportModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber), PARAMETER_SOURCE_SCHEMA, pSourceSchema, PARAMETER_SSH_HOST, PARAMETER_SSH_HOST,
        pSSHHost, PARAMETER_SSH_USER, pSSHUser, PARAMETER_SSH_PASSWORD, MESSAGE_FILTERED, PARAMETER_SSH_PORT,
        pSSHPortNumber);
      return new Oracle12cJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword, pSourceSchema,
        pSSHHost, pSSHUser, pSSHPassword, pSSHPortNumber);
    } else {
      reporter.exportModuleParameters(getModuleName(), PARAMETER_SERVER_NAME, pServerName, PARAMETER_INSTANCE,
        pDatabase, PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        Integer.toString(pPortNumber), PARAMETER_SOURCE_SCHEMA, pSourceSchema);
      return new Oracle12cJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword, pSourceSchema);
    }
  }

  private String getLicenseText(String parameter) {
    return "Please agree to the Oracle Technology Network Development and Distribution License Terms before using this module.\n"
      + "The Oracle Technology Network Development and Distribution License Terms are available at\n" + LICENSE_URL
      + "\nTo agree you must specify the additional parameter " + parameter + " in your command.";
  }
}
