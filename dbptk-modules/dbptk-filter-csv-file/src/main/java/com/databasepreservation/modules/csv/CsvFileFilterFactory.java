/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.csv;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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

public class CsvFileFilterFactory implements DatabaseFilterFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_PRINT_HEADER = "print-header";
  public static final String PARAMETER_SEPARATOR = "separator";
  public static final String PARAMETER_MESSAGE_DIGEST_ALGORITHM = "digest";
  public static final String PARAMETER_EXPLAIN = "explain";
  public static final String PARAMETER_FONT_CASE = "font-case";



  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to save the csv file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter printHeader = new Parameter().shortName("h")
          .longName(PARAMETER_PRINT_HEADER)
          .description("The message to indicate if the header should be printed or not (Default: true)")
          .hasArgument(false).setOptionalArgument(false).required(false).valueIfSet("false").valueIfNotSet("true");

  private static final Parameter separator = new Parameter().shortName("s")
          .longName(PARAMETER_SEPARATOR)
          .description("Character that indicates the table separator of the csv file")
          .hasArgument(true).setOptionalArgument(false).required(false).valueIfNotSet(",");

  @Override
  public String getFilterName() {
    return "csv-file";
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
    return new Parameters(Arrays.asList(file.inputType(Parameter.INPUT_TYPE.FILE_SAVE).fileFilter(Parameter.FILE_FILTER_TYPE.JSON_EXTENSION),
      printHeader.inputType(Parameter.INPUT_TYPE.COMBOBOX).possibleValues("true","false").defaultSelectedIndex(0),
      separator.inputType(Parameter.INPUT_TYPE.TEXT)),
      null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(printHeader.longName(), printHeader);
    parameterHashMap.put(separator.longName(), separator);


    return parameterHashMap;
  }

  @Override
  public DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    // save file path
    Path pFile = Paths.get(parameters.get(file));

    // value to print header
    boolean pPrintHeader = Boolean.parseBoolean(parameters.get(printHeader));

    // font case
    String pSeparator = separator.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(separator))) {
      pSeparator = parameters.get(separator);
    }
    ModuleUtils.validateSeparator(pSeparator);




    reporter.filterParameters(getFilterName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
            PARAMETER_PRINT_HEADER, Boolean.toString(pPrintHeader),
            PARAMETER_SEPARATOR, pSeparator);

    return new CsvFileFilter(pFile,pPrintHeader,pSeparator);
  }
}
