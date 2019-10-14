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

import com.databasepreservation.RemoteConnectionManager;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
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
  public static final String PARAMETER_SSH = "ssh";
  public static final String PARAMETER_SSH_HOST = "ssh-host";
  public static final String PARAMETER_SSH_USER = "ssh-user";
  public static final String PARAMETER_SSH_PASSWORD = "ssh-password";
  public static final String PARAMETER_SSH_PORT = "ssh-port";

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
    return new Parameters(Arrays.asList(hostname.inputType(INPUT_TYPE.TEXT), portNumber.inputType(INPUT_TYPE.NUMBER),
      username.inputType(INPUT_TYPE.TEXT), password.inputType(INPUT_TYPE.PASSWORD),
      database.inputType(INPUT_TYPE.TEXT), disableEncryption.inputType(INPUT_TYPE.CHECKBOX)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, portNumber, database, username, password, disableEncryption, ssh, sshHost, sshUser,
      sshPassword, sshPort, customViews), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(
      Arrays.asList(hostname, portNumber, database, username, password, disableEncryption, ssh, sshHost, sshUser, sshPassword, sshPort),
      null);
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
        PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
          String.valueOf(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt), PARAMETER_SSH_HOST, pSSHHost, PARAMETER_SSH_USER, pSSHUser, PARAMETER_SSH_PASSWORD,
        reporter.MESSAGE_FILTERED, PARAMETER_SSH_PORT, pSSHPortNumber);
      return new MySQLJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt, true, pSSHHost,
        pSSHUser, pSSHPassword, pSSHPortNumber, pCustomViews);
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
        PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
        String.valueOf(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt));
      return new MySQLJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt, pCustomViews);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
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
      reporter.exportModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
          PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
          Integer.toString(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt), PARAMETER_SSH_HOST, pSSHHost, PARAMETER_SSH_USER, pSSHUser, PARAMETER_SSH_PASSWORD,
          reporter.MESSAGE_FILTERED, PARAMETER_SSH_PORT, pSSHPortNumber);
      return new MySQLJDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt,true, pSSHHost,
          pSSHUser, pSSHPassword, pSSHPortNumber);
    } else {
      reporter.exportModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
          PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED, PARAMETER_PORT_NUMBER,
          Integer.toString(pPortNumber), PARAMETER_DISABLE_ENCRYPTION, String.valueOf(pEncrypt));

      return new MySQLJDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt);
    }
  }
}
