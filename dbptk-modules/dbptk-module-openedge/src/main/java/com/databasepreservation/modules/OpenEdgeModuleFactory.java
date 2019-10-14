/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.RemoteConnectionManager;
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
import com.databasepreservation.modules.in.OpenEdgeJDBCImportModule;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class OpenEdgeModuleFactory implements DatabaseModuleFactory {

  public static final String PARAMETER_USERNAME = "username";
  public static final String PARAMETER_PASSWORD = "password";
  public static final String PARAMETER_HOSTNAME = "hostname";
  public static final String PARAMETER_DATABASE = "database";
  public static final String PARAMETER_PORT_NUMBER = "port-number";
  public static final String PARAMETER_CUSTOM_VIEWS = "custom-views";
  public static final String PARAMETER_DRIVER = "driver";
  public static final String PARAMETER_SSH = "ssh";
  public static final String PARAMETER_SSH_HOST = "ssh-host";
  public static final String PARAMETER_SSH_USER = "ssh-user";
  public static final String PARAMETER_SSH_PASSWORD = "ssh-password";
  public static final String PARAMETER_SSH_PORT = "ssh-port";

  private static final Parameter username = new Parameter().shortName("u").longName(PARAMETER_USERNAME)
      .description("the name of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
      .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
      .description("the password of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
      .required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the server port number").hasArgument(true).setOptionalArgument(true).required(false)
    .valueIfNotSet("20931");

  private static final Parameter database = new Parameter().shortName("db").longName(PARAMETER_DATABASE)
      .description("the name of the database we'll be accessing").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter hostname = new Parameter().shortName("h").longName(PARAMETER_HOSTNAME)
      .description("the name (host name) of the server").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter customViews = new Parameter().shortName("cv").longName(PARAMETER_CUSTOM_VIEWS)
          .description("the path to a custom view query list file").hasArgument(true).setOptionalArgument(false)
          .required(false);

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
    return false;
  }

  @Override
  public String getModuleName() {
    return "progress-openedge";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(ssh.longName(), ssh);
    parameterHashMap.put(sshHost.longName(), sshHost);
    parameterHashMap.put(sshUser.longName(), sshUser);
    parameterHashMap.put(sshPassword.longName(), sshPassword);
    parameterHashMap.put(sshPort.longName(), sshPort);
    parameterHashMap.put(customViews.longName(), customViews);

    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    final Parameter driver = new Parameter().shortName("d").longName(PARAMETER_DRIVER)
      .description("the location of the JDBC driver").hasArgument(true).setOptionalArgument(false).required(true)
      .inputType(INPUT_TYPE.DRIVER);
    return new Parameters(Arrays.asList(driver, hostname.inputType(INPUT_TYPE.TEXT),
      portNumber.inputType(INPUT_TYPE.NUMBER), username.inputType(INPUT_TYPE.TEXT),
      password.inputType(INPUT_TYPE.PASSWORD), database.inputType(INPUT_TYPE.TEXT)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, database, username, password, portNumber, ssh, sshHost, sshUser,
      sshPassword, sshPort, customViews), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return null;
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    String pUsername    = parameters.get(username);
    String pPassword    = parameters.get(password);
    String pHostname    = parameters.get(hostname);
    String pDatabase    = parameters.get(database);

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    Path pCustomViews = null;
    if (StringUtils.isNotBlank(parameters.get(customViews))) {
      pCustomViews = Paths.get(parameters.get(customViews));
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

    if (pSSH) {
      RemoteConnectionManager.getInstance().setup(pSSHHost, pSSHUser, pSSHPassword, pSSHPortNumber);
      reporter.importModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
        PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_SSH_HOST, pSSHHost,
        PARAMETER_SSH_USER, pSSHUser, PARAMETER_SSH_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        pPortNumber.toString(), PARAMETER_SSH_PORT, pSSHPortNumber);
      return new OpenEdgeJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, true, pSSHHost,
        pSSHUser, pSSHPassword, pSSHPortNumber, pCustomViews);
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
          PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
          pPortNumber.toString());
      return new OpenEdgeJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pCustomViews);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) throws UnsupportedModuleException, LicenseNotAcceptedException {
    return null;
  }
}
