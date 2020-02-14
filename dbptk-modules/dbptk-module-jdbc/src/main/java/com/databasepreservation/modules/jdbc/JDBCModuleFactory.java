/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.jdbc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.jdbc.in.JDBCImportModule;
import com.databasepreservation.modules.jdbc.out.JDBCExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class JDBCModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_DRIVER = "driver";
  public static final String PARAMETER_CONNECTION = "connection";
  private static final String PARAMETER_DRIVER_CLASS = "class";

  private static final Parameter driver = new Parameter().shortName("d").longName(PARAMETER_DRIVER)
    .description("the name of the JDBC driver class. For more info about this refer to the website or the README file")
    .hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter connection = new Parameter().shortName("c").longName(PARAMETER_CONNECTION)
    .description("the connection url to use in the connection").hasArgument(true).setOptionalArgument(false)
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
    return "jdbc";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(driver.longName(), driver);
    parameterHashMap.put(connection.longName(), connection);

    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    final Parameter driverJAR = new Parameter().shortName("d").longName(PARAMETER_DRIVER)
      .description("the location of the JDBC driver").hasArgument(true).setOptionalArgument(false).required(true)
      .inputType(INPUT_TYPE.DRIVER);
    return new Parameters(Arrays.asList(driverJAR, driver.longName(PARAMETER_DRIVER_CLASS).inputType(INPUT_TYPE.TEXT),
      connection.inputType(INPUT_TYPE.TEXT)), null);
  }

  @Override
  public Parameters getImportModuleParameters() {
    return new Parameters(Arrays.asList(driver, connection), null);
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(Arrays.asList(driver, connection), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) {
    String pDriver = parameters.get(driver);
    String pConnection = parameters.get(connection);

    reporter.importModuleParameters(this.getModuleName(), PARAMETER_DRIVER, pDriver, PARAMETER_CONNECTION, pConnection);
    return new JDBCImportModule(pDriver, pConnection);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) {
    String pDriver = parameters.get(driver);
    String pConnection = parameters.get(connection);

    reporter.exportModuleParameters(this.getModuleName(), PARAMETER_DRIVER, pDriver, PARAMETER_CONNECTION, pConnection);
    return new JDBCExportModule(pDriver, pConnection);
  }
}
