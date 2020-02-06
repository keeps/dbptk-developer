/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.CATEGORY_TYPE;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.input.SIARDDKImportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FOLDER = "folder";
  public static final String PARAMETER_ARCHIVE_INDEX = "archiveIndex";
  public static final String PARAMETER_CONTEXT_DOCUMENTATION_INDEX = "contextDocumentationIndex";
  public static final String PARAMETER_CONTEXT_DOCUMENTATION_FOLDER = SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER;
  public static final String PARAMETER_AS_SCHEMA = "as-schema";
  public static final String PARAMETER_LOBS_PER_FOLDER = "lobs-per-folder";
  public static final String PARAMETER_LOBS_FOLDER_SIZE = "lobs-folder-size";

  // TODO: As things are now, are we not always generating the '.1' version of
  // the archive (indicating that the last .[1-9][0-9] should perhaps not be
  // inputed by the user - but added by the code automatically? )
  private static final Parameter folder = new Parameter().shortName("f").longName(PARAMETER_FOLDER).description(
    "Path to (the first) SIARDDK archive folder. Archive folder name must match the expression AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.1 Any additional parts of the archive (eg. with suffixes .2 .3 etc) referenced in the tableIndex.xml will also be processed.")
    .hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter archiveIndex = new Parameter().shortName("ai").longName(PARAMETER_ARCHIVE_INDEX)
    .description("Path to archiveIndex.xml input file").hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter contextDocumentationIndex = new Parameter().shortName("ci")
    .longName(PARAMETER_CONTEXT_DOCUMENTATION_INDEX).description("Path to contextDocumentationIndex.xml input file")
    .hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter contextDocumentationFolder = new Parameter().shortName("cf")
    .longName(PARAMETER_CONTEXT_DOCUMENTATION_FOLDER)
    .description("Path to contextDocumentation folder which should contain the context documentation for the archive")
    .hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter PARAM_IMPORT_AS_SCHEMA = new Parameter().shortName("as").longName(PARAMETER_AS_SCHEMA)
    .description(
      "Name of the database schema to use when importing the SIARDDK archive. Suggested values: PostgreSQL:'public', MySQL:'<name of database>', MSSQL:'dbo'")
    .required(true).hasArgument(true);

  private static final Parameter lobsPerFolder = new Parameter().shortName("lpf").longName(PARAMETER_LOBS_PER_FOLDER)
    .description("The maximum number of documents (i.e. folders) present in a docCollection folder (default is 10000).")
    .required(false).hasArgument(true).setOptionalArgument(false).valueIfNotSet("10000");

  private static final Parameter lobsFolderSize = new Parameter().shortName("lfs").longName(PARAMETER_LOBS_FOLDER_SIZE)
    .description("The maximum size (in megabytes) of the docCollection folders (default is 1000 MB").required(false)
    .hasArgument(true).setOptionalArgument(false).valueIfNotSet("1000");

  // This is not used now, but will be used later
  // private static final Parameter clobType = new
  // Parameter().shortName("ct").longName("clobtype")
  // .description("Specify the type for
  // CLOBs").hasArgument(true).setOptionalArgument(false).required(false)
  // .valueIfNotSet(SIARDDKConstants.DEFAULT_CLOB_TYPE);

  // This is not used now, but will be used later
  // private static final Parameter clobLength = new
  // Parameter().shortName("cl").longName("cloblength")
  // .description("The threshold length of CLOBs before converting to
  // tiff").hasArgument(true)
  // .setOptionalArgument(false).required(false).valueIfNotSet(SIARDDKConstants.DEFAULT_MAX_CLOB_LENGTH);

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
    return "siard-dk";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterMap = new HashMap<String, Parameter>();

    parameterMap.put(folder.longName(), folder);
    parameterMap.put(archiveIndex.longName(), archiveIndex);
    parameterMap.put(contextDocumentationIndex.longName(), contextDocumentationIndex);
    parameterMap.put(contextDocumentationFolder.longName(), contextDocumentationFolder);
    parameterMap.put(PARAM_IMPORT_AS_SCHEMA.longName(), PARAM_IMPORT_AS_SCHEMA);
    parameterMap.put(lobsPerFolder.longName(), lobsPerFolder);
    parameterMap.put(lobsFolderSize.longName(), lobsFolderSize);
    // to be used later...
    // parameterMap.put(clobType.longName(), clobType);
    // parameterMap.put(clobLength.longName(), clobLength);

    return parameterMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(
      Arrays.asList(folder.inputType(INPUT_TYPE.FOLDER), PARAM_IMPORT_AS_SCHEMA.inputType(INPUT_TYPE.TEXT)), null);
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(folder, PARAM_IMPORT_AS_SCHEMA), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    // return new Parameters(Arrays.asList(folder, archiveIndex,
    // contextDocumentationIndex, contextDocmentationFolder,
    // clobType, clobLength), null);

    return new Parameters(
      Arrays.asList(folder.inputType(INPUT_TYPE.FOLDER).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
        archiveIndex.inputType(INPUT_TYPE.FILE_OPEN).fileFilter(Parameter.FILE_FILTER_TYPE.XML_EXTENSION)
          .exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
        contextDocumentationIndex.inputType(INPUT_TYPE.FILE_OPEN).fileFilter(Parameter.FILE_FILTER_TYPE.XML_EXTENSION)
          .exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
        contextDocumentationFolder.inputType(INPUT_TYPE.FOLDER).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
        lobsPerFolder.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
        lobsFolderSize.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS)),
      null);

  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) {
    return buildImportModule(parameters, ModuleConfigurationUtils.getDefaultModuleConfiguration(), reporter);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters,
    ModuleConfiguration moduleConfiguration, Reporter reporter) {
    reporter.importModuleParameters(getModuleName(), "file",
      Paths.get(parameters.get(folder)).normalize().toAbsolutePath().toString(), PARAM_IMPORT_AS_SCHEMA.longName(),
      parameters.get(PARAM_IMPORT_AS_SCHEMA));
    return new SIARDDKImportModule(moduleConfiguration, Paths.get(parameters.get(folder)), parameters.get(PARAM_IMPORT_AS_SCHEMA))
      .getDatabaseImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {

    // Get the values passed to the parameter flags from the command line

    String pFolder = parameters.get(folder);
    String pArchiveIndex = parameters.get(archiveIndex);
    String pContextDocumentationIndex = parameters.get(contextDocumentationIndex);
    String pContextDocumentationFolder = parameters.get(contextDocumentationFolder);

    // optional
    String pLobsPerFolder = lobsPerFolder.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(lobsPerFolder))) {
      pLobsPerFolder = parameters.get(lobsPerFolder);
    }

    // optional
    String pLobsFolderSize = lobsFolderSize.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(lobsFolderSize))) {
      pLobsFolderSize = parameters.get(lobsFolderSize);
    }

    // to be used later...
    // String pClobType = parameters.get(clobType);
    // String pClobLength = parameters.get(clobLength);

    Map<String, String> exportModuleArgs = new HashMap<String, String>();
    exportModuleArgs.put(folder.longName(), pFolder);
    // exportModuleArgs.put(tableFilter.longName(), pTableFilter.toString());
    exportModuleArgs.put(archiveIndex.longName(), pArchiveIndex);
    exportModuleArgs.put(contextDocumentationIndex.longName(), pContextDocumentationIndex);
    exportModuleArgs.put(contextDocumentationFolder.longName(), pContextDocumentationFolder);
    exportModuleArgs.put(lobsPerFolder.longName(), pLobsPerFolder);
    exportModuleArgs.put(lobsFolderSize.longName(), pLobsFolderSize);

    // to be used later...
    // exportModuleArgs.put(clobType.longName(), pClobType);
    // exportModuleArgs.put(clobLength.longName(), pClobLength);

    List<String> exportModuleParameters = new ArrayList<String>();
    exportModuleParameters.add(folder.longName());
    exportModuleParameters.add(pFolder);
    exportModuleParameters.add(archiveIndex.longName());
    exportModuleParameters.add(pArchiveIndex);
    exportModuleParameters.add(contextDocumentationIndex.longName());
    exportModuleParameters.add(pContextDocumentationIndex);
    exportModuleParameters.add(contextDocumentationFolder.longName());
    exportModuleParameters.add(pContextDocumentationFolder);
    if (!pLobsPerFolder.equals(lobsPerFolder.valueIfNotSet())) {
      exportModuleParameters.add(lobsPerFolder.longName());
      exportModuleParameters.add(pLobsPerFolder);
    }
    if (!pLobsFolderSize.equals(lobsFolderSize.valueIfNotSet())) {
      exportModuleParameters.add(lobsFolderSize.longName());
      exportModuleParameters.add(pLobsFolderSize);
    }
    reporter.exportModuleParameters(getModuleName(), exportModuleParameters.toArray(new String[0]));

    return new SIARDDKExportModule(exportModuleArgs).getDatabaseExportModule();
  }
}
