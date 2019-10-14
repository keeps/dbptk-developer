/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.databasepreservation.Constants;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerRemoteFileSystem;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerFileSystem;
import com.databasepreservation.modules.listTables.ListTables;

public class ExternalLOBSFilterFactory implements DatabaseFilterFactory {
  public final Pattern LINE_PATTERN = Pattern
          .compile("^([^\\s\\.\\{\\}\\;]+)\\.([^\\s\\.\\{\\}\\;]+)\\{((?:[^\\s\\.\\{\\}\\;]*\\;)*[^\\s\\.\\{\\}\\;]*)\\}$");

  public static final String PARAMETER_COLUMN_LIST = "column-list";
  public static final String PARAMETER_BASE_PATH = "base-path";
  public static final String PARAMETER_REFERENCE_TYPE = "reference-type";
  private static final String PARAMETER_COLUMN_LIST_CONTENT = "column-list-content";

  private static final Parameter colList = new Parameter().shortName("cl").longName(PARAMETER_COLUMN_LIST).description(
    "file with the list of columns that refer to external LOBs (this file uses the same format as the one created by the list-tables export module).")
    .required(true).hasArgument(true).setOptionalArgument(false);

  private static final Parameter referenceType = new Parameter().shortName("t").longName(PARAMETER_REFERENCE_TYPE)
    .description("type of the references found in the listed columns (supported types: 'file-system', 'remote-file-system')").required(true)
    .hasArgument(true).setOptionalArgument(false);

  private static final Parameter basePath = new Parameter().shortName("bp").longName(PARAMETER_BASE_PATH)
    .description("base path to use in case columns contain relative paths").required(false).hasArgument(true)
    .setOptionalArgument(false).valueIfNotSet("");


  @Override
  public String getFilterName() {
    return "external-lobs";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Arrays.asList(colList, referenceType, basePath), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(colList.longName(), colList);
    parameterHashMap.put(referenceType.longName(), referenceType);
    parameterHashMap.put(basePath.longName(), basePath);

    return parameterHashMap;
  }

  @Override
  public DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    StringBuilder content = new StringBuilder();
    Path pColList = Paths.get(parameters.get(colList));
    Map<String, Map<String, List<String>>> parsedColList = parseColumnList(pColList, content);

    String pCellHandlerType = parameters.get(referenceType);

    // initialize cell handler to be used in the filter
    ExternalLOBSCellHandler cellHandler;

    Path pBasePath = Paths.get(basePath.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(basePath))) {
      pBasePath = Paths.get(parameters.get(basePath));
    }

    if ("file-system".equalsIgnoreCase(pCellHandlerType)) {
      cellHandler = new ExternalLOBSCellHandlerFileSystem(pBasePath, reporter);
      report(reporter, pColList, content.toString(), pBasePath, pCellHandlerType);
    } else if ("remote-file-system".equalsIgnoreCase(pCellHandlerType)) {
      cellHandler = new ExternalLOBSCellHandlerRemoteFileSystem(pBasePath, reporter);
      report(reporter, pColList, content.toString(), pBasePath, pCellHandlerType);
    } else {
      throw new ModuleException().withMessage("Unrecognized reference type " + pCellHandlerType);
    }

    return new ExternalLOBSFilter(parsedColList, cellHandler);
  }

  private Map<String, Map<String, List<String>>> parseColumnList(Path columnListPath, StringBuilder content) throws ModuleException {
    Map<String, Map<String, List<String>>> colList = new HashMap<>();

    if (columnListPath != null) {
      try (InputStream inputStream = Files.newInputStream(columnListPath)) {
        // attempt to get a col list from the file at columnListPath

        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(inputStreamReader);

        String line;
        while ((line = reader.readLine()) != null) {
          if (StringUtils.isNotBlank(line)) {
            content.append(line).append(Constants.NEW_LINE);

            Matcher lineMatcher = ListTables.LINE_PATTERN.matcher(line);
            if (!lineMatcher.matches()) {
              throw new ModuleException().withMessage("Malformed entry in column list: " + line);
            }

            String schemaPart = lineMatcher.group(1);
            String tablePart = lineMatcher.group(2);
            tablePart = tablePart.substring(1);
            String columnPart = lineMatcher.group(3);
            String[] columns = columnPart.split(ListTables.COLUMNS_SEPARATOR);

            if (colList.containsKey(schemaPart)) {
              if (colList.get(schemaPart).containsKey(tablePart)) {
                colList.get(schemaPart).get(tablePart).addAll(Arrays.asList(columns));
              } else {
                colList.get(schemaPart).put(tablePart, new ArrayList<>(Arrays.asList(columns)));
              }
            } else {
              HashMap<String, List<String>> newSchema = new HashMap<>();
              newSchema.put(tablePart, new ArrayList<>(Arrays.asList(columns)));
              colList.put(schemaPart, newSchema);
            }
          }
        }
      } catch (IOException e) {
        throw new ModuleException()
          .withMessage("Could not read col list from file " + columnListPath.toAbsolutePath().toString()).withCause(e);
      }
    }

    return colList;
  }

  private void report(Reporter reporter, Path pColList, String content, Path pBasePath, String pCellHandlerType) {
    reporter.filterParameters(getFilterName(),
        PARAMETER_COLUMN_LIST, pColList.normalize().toAbsolutePath().toString(),
        PARAMETER_COLUMN_LIST_CONTENT, content,
        PARAMETER_BASE_PATH, pBasePath.normalize().toAbsolutePath().toString(),
        PARAMETER_REFERENCE_TYPE, pCellHandlerType);
  }
}
