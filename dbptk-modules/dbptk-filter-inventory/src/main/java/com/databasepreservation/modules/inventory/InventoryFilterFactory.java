/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.inventory;

import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.databasepreservation.utils.ConfigUtils;

public class InventoryFilterFactory implements DatabaseFilterFactory {
  public static final String PARAMETER_PREFIX = "prefix";
  public static final String PARAMETER_DIR = "dir";
  public static final String PARAMETER_DISABLE_PRINT_HEADER = "disable-print-header";
  public static final String PARAMETER_SEPARATOR = "separator";

  private static final Parameter prefix = new Parameter().shortName("pr").longName(PARAMETER_PREFIX)
    .description("Prefix to append to inventory files").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter dir = new Parameter().shortName("dp").longName(PARAMETER_DIR)
    .description("Dir path to save inventory files").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter printHeader = new Parameter().shortName("ph").longName(PARAMETER_DISABLE_PRINT_HEADER)
    .description("Value to indicate if the header should be printed or not (Default: true)").hasArgument(false)
    .setOptionalArgument(false).required(false).valueIfSet("false").valueIfNotSet("true");

  private static final Parameter separator = new Parameter().shortName("sp").longName(PARAMETER_SEPARATOR)
    .description("Character that indicates the table separator of the inventory file").hasArgument(true)
    .setOptionalArgument(false).required(false).valueIfNotSet(",");

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
    return new Parameters(
      Arrays.asList(prefix.inputType(Parameter.INPUT_TYPE.TEXT), dir.inputType(Parameter.INPUT_TYPE.TEXT),
        printHeader.inputType(Parameter.INPUT_TYPE.COMBOBOX).possibleValues("true", "false").defaultSelectedIndex(0),
        separator.inputType(Parameter.INPUT_TYPE.TEXT)),
      null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(prefix.longName(), prefix);
    parameterHashMap.put(dir.longName(), dir);
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
    Path pDir = ConfigUtils.getHomeDirectory();
    if (StringUtils.isNotBlank(parameters.get(dir))) {
      pDir = Paths.get(parameters.get(dir));
    }

    if (!pDir.toFile().exists()) {
      if (!pDir.toFile().mkdirs()) {
        throw new ModuleException().withMessage("Can not create dirs to path: '" + pDir.toString() + "'");
      }
    }

    // value to print header
    boolean pPrintHeader = Boolean.parseBoolean(printHeader.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(printHeader))) {
      pPrintHeader = Boolean.parseBoolean(printHeader.valueIfSet());
    }

    // separator
    String pSeparator = separator.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(separator))) {
      pSeparator = parameters.get(separator);
    }

    if (pSeparator.length() > 1) {
      throw new ModuleException()
        .withMessage("Separator: '" + pSeparator + "' is not supported type for Csv delimiter");
    }

    reporter.filterParameters(getFilterName(), PARAMETER_PREFIX, pPrefix, PARAMETER_DIR, pDir.toString(),
      PARAMETER_DISABLE_PRINT_HEADER, Boolean.toString(pPrintHeader), PARAMETER_SEPARATOR, pSeparator);

    return new InventoryFilter(pPrefix, pDir, pPrintHeader, pSeparator);
  }
}
