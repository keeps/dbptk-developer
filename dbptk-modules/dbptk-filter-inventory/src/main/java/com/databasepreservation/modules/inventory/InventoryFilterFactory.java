/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.inventory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.modules.filters.ExecutionOrder;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.utils.ModuleUtils;

public class InventoryFilterFactory implements DatabaseFilterFactory {
  public static final String PARAMETER_PREFIX = "prefix";
  public static final String PARAMETER_DIR_PATH = "dir-path";
  public static final String PARAMETER_PRINT_HEADER = "print-header";
  public static final String PARAMETER_SEPARATOR = "separator";


  private static final Parameter prefix = new Parameter().shortName("pr").longName(PARAMETER_PREFIX)
    .description("Prefix to append to inventory files").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter dirPath = new Parameter().shortName("dp").longName(PARAMETER_DIR_PATH)
          .description("Dir path to save inventory files").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter printHeader = new Parameter().shortName("ph")
          .longName(PARAMETER_PRINT_HEADER)
          .description("The message to indicate if the header should be printed or not (Default: true)")
          .hasArgument(false).setOptionalArgument(false).required(false).valueIfSet("false").valueIfNotSet("true");

  private static final Parameter separator = new Parameter().shortName("sp")
          .longName(PARAMETER_SEPARATOR)
          .description("Character that indicates the table separator of the inventory file")
          .hasArgument(false).setOptionalArgument(false).required(false).valueIfNotSet(",").valueIfSet(";");

  @Override
  public String getFilterName() {
    return "inventory";
  }

  @Override
  public ExecutionOrder getExecutionOrder() {
    return ExecutionOrder.AFTER;
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Arrays.asList(prefix.inputType(Parameter.INPUT_TYPE.TEXT), dirPath.inputType(Parameter.INPUT_TYPE.TEXT),
      printHeader.inputType(Parameter.INPUT_TYPE.COMBOBOX).possibleValues("true","false").defaultSelectedIndex(0),
      separator.inputType(Parameter.INPUT_TYPE.TEXT)),
      null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(prefix.longName(), prefix);
    parameterHashMap.put(dirPath.longName(), dirPath);
    parameterHashMap.put(printHeader.longName(), printHeader);
    parameterHashMap.put(separator.longName(), separator);

    return parameterHashMap;
  }

  @Override
  public DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {

    // value to prefix
    String pPrefix = "";
    if (StringUtils.isNotBlank(parameters.get(prefix))) {
      pPrefix = parameters.get(prefix);
    }

    // dir path to save inventory
    String pDirPath = "";
    if (StringUtils.isNotBlank(parameters.get(dirPath))) {
      pDirPath = parameters.get(dirPath);
    }

    boolean pPrintHeader= Boolean.parseBoolean(this.printHeader.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(this.printHeader))) {
      pPrintHeader = Boolean.parseBoolean(this.printHeader.valueIfSet());
    }


    // separator
    char pSeparator = separator.valueIfNotSet().charAt(0);
    if (StringUtils.isNotBlank(parameters.get(separator))) {
      pSeparator = parameters.get(separator).charAt(0);
    }


    reporter.filterParameters(getFilterName(), PARAMETER_PREFIX, pPrefix,
            PARAMETER_DIR_PATH, pDirPath,
            PARAMETER_PRINT_HEADER, Boolean.toString(pPrintHeader),
            PARAMETER_SEPARATOR, String.valueOf(pSeparator));

    return new InventoryFilter(pPrefix,pDirPath,pPrintHeader,pSeparator);
  }
}
