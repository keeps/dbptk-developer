package com.databasepreservation.modules.mySql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.mySql.in.MySQLJDBCImportModule;
import com.databasepreservation.modules.mySql.out.MySQLJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySQLModuleFactory implements DatabaseModuleFactory {
  private static final Parameter hostname = new Parameter().shortName("h").longName("hostname")
    .description("the hostname of the MySQL server").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName("port-number")
    .description("the port that the MySQL server is listening").hasArgument(true).setOptionalArgument(false)
    .required(false);

  private static final Parameter database = new Parameter().shortName("db").longName("database")
    .description("the name of the MySQL database").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter username = new Parameter().shortName("u").longName("username")
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName("password")
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private Reporter reporter;

  private MySQLModuleFactory() {
  }

  public MySQLModuleFactory(Reporter reporter) {
    this.reporter = reporter;
  }

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
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(portNumber.longName(), portNumber);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, database, username, password, portNumber), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, database, username, password, portNumber), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    }

    if (pPortNumber == null) {
      reporter.importModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
        pUsername, "password", reporter.MESSAGE_FILTERED);
      return new MySQLJDBCImportModule(pHostname, pDatabase, pUsername, pPassword);
    } else {
      reporter.importModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
        pUsername, "password", reporter.MESSAGE_FILTERED, "port number", pPortNumber.toString());
      return new MySQLJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    }

    if (pPortNumber == null) {
      reporter.exportModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
        pUsername, "password", reporter.MESSAGE_FILTERED);
      return new MySQLJDBCExportModule(pHostname, pDatabase, pUsername, pPassword);
    } else {
      reporter.exportModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
        pUsername, "password", reporter.MESSAGE_FILTERED, "port number", pPortNumber.toString());
      return new MySQLJDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword);
    }
  }
}
