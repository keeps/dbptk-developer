package com.databasepreservation.modules.postgreSql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.postgreSql.in.PostgreSQLJDBCImportModule;
import com.databasepreservation.modules.postgreSql.out.PostgreSQLJDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSQLModuleFactory implements DatabaseModuleFactory {
  private static final Parameter hostname = new Parameter().shortName("h").longName("hostname")
    .description("the name of the PostgreSQL server host (e.g. localhost)").hasArgument(true)
    .setOptionalArgument(false).required(true);

  private static final Parameter database = new Parameter().shortName("db").longName("database")
    .description("the name of the database to connect to").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter username = new Parameter().shortName("u").longName("username")
    .description("the name of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter password = new Parameter().shortName("p").longName("password")
    .description("the password of the user to use in connection").hasArgument(true).setOptionalArgument(false)
    .required(true);

  private static final Parameter disableEncryption = new Parameter().shortName("de").longName("disable-encryption")
    .description("use to turn off encryption in the connection").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter portNumber = new Parameter().shortName("pn").longName("port-number")
    .description("the port of where the PostgreSQL server is listening, default is 5432").hasArgument(true)
    .setOptionalArgument(false).required(false).valueIfNotSet("5432");

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
    return "postgresql";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(database.longName(), database);
    parameterHashMap.put(username.longName(), username);
    parameterHashMap.put(password.longName(), password);
    parameterHashMap.put(disableEncryption.longName(), disableEncryption);
    parameterHashMap.put(portNumber.longName(), portNumber);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(hostname, database, username, password, disableEncryption, portNumber), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(hostname, database, username, password, disableEncryption, portNumber), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    Reporter.importModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
      pUsername, "password", Reporter.MESSAGE_FILTERED_PASSWORD, "port number", pPortNumber.toString());
    return new PostgreSQLJDBCImportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {
    String pHostname = parameters.get(hostname);
    String pDatabase = parameters.get(database);
    String pUsername = parameters.get(username);
    String pPassword = parameters.get(password);

    // boolean
    boolean pEncrypt = !Boolean.parseBoolean(parameters.get(disableEncryption));

    // optional
    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(portNumber))) {
      pPortNumber = Integer.parseInt(parameters.get(portNumber));
    } else {
      pPortNumber = Integer.parseInt(portNumber.valueIfNotSet());
    }

    Reporter.exportModuleParameters(getModuleName(), "hostname", pHostname, "database", pDatabase, "username",
      pUsername, "password", Reporter.MESSAGE_FILTERED_PASSWORD, "port number", pPortNumber.toString());
    return new PostgreSQLJDBCExportModule(pHostname, pPortNumber, pDatabase, pUsername, pPassword, pEncrypt);
  }
}
