package com.databasepreservation.modules.db2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.db2.in.DB2JDBCImportModule;
import com.databasepreservation.modules.db2.out.DB2JDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DB2ModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_HOSTNAME = "hostname";
  public static final String PARAMETER_PORT_NUMBER = "port-number";
  public static final String PARAMETER_DATABASE = "database";
  public static final String PARAMETER_USERNAME = "username";
  public static final String PARAMETER_PASSWORD = "password";

  private static final Parameter hostname = new Parameter().shortName("h").longName(PARAMETER_HOSTNAME)
    .description("the host name of Db2 Server").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter portNumber = new Parameter().shortName("pn").longName(PARAMETER_PORT_NUMBER)
    .description("the port that the Db2 server is listening").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter database = new Parameter().shortName("db").longName(PARAMETER_DATABASE)
    .description("the name of the database to import from").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter username = new Parameter().shortName("u").longName(PARAMETER_USERNAME)
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName(PARAMETER_PASSWORD)
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

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
    return "db2";
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
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);
    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    reporter.importModuleParameters(this.getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
      PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED);
    return new DB2JDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);
    Integer pPortNumber = Integer.parseInt(parameters.get(portNumber));

    reporter.exportModuleParameters(this.getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_DATABASE, pDatabase,
      PARAMETER_USERNAME, pUsername, PARAMETER_PASSWORD, reporter.MESSAGE_FILTERED);
    return new DB2JDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword);
  }
}
