package com.databasepreservation.modules.siard;

import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.input.SIARDDKImportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class SIARDDKModuleFactory implements DatabaseModuleFactory {
  // TODO: As things are now, are we not always generating the '.1' version of
  // the archive (indicating that the last .[1-9][0-9] should perhaps not be
  // inputed by the user - but added by the code automatically? )
  public static final Parameter folder = new Parameter()
    .shortName("f")
    .longName("folder")
    .description(
      "Path to SIARDDK archive folder. Archive folder name must match the expression AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.[1-9][0-9]")
    .hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter tableFilter = new Parameter()
    .shortName("tf")
    .longName("table-filter")
    .description(
      "file with the list of tables that should be exported (this file can be created by the list-tables export module).")
    .required(false).hasArgument(true).setOptionalArgument(false);

  public static final Parameter archiveIndex = new Parameter().shortName("ai").longName("archiveIndex")
    .description("Path to archiveIndex.xml input file").hasArgument(true).setOptionalArgument(false).required(false);

  public static final Parameter contextDocumentationIndex = new Parameter().shortName("ci")
    .longName("contextDocumentationIndex").description("Path to contextDocumentationIndex.xml input file")
    .hasArgument(true).setOptionalArgument(false).required(false);

  public static final Parameter contextDocmentationFolder = new Parameter().shortName("cf")
    .longName(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER)
    .description("Path to contextDocumentation folder which should contain the context documentation for the archive")
    .hasArgument(true).setOptionalArgument(false).required(false);

  public static final Parameter PARAM_IMPORT_AS_SCHEMA = new Parameter()
    .shortName("as")
    .longName("as-schema")
    .description(
      "Name of the database schema to use when importing the SIARDDK archive. Suggested values: PostgreSQL:'public', MySQL:'<name of database>', MSSQL:'dbo'")
    .required(true).hasArgument(true);

  public static final Parameter PARAM_IMPORT_FOLDER = new Parameter()
    .shortName("f")
    .longName("folder")
    .description(
      "Path to (the first) SIARDDK archive folder. Archive folder name must match the expression AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.1 Any additional parts of the archive (eg. with suffixes .2 .3 etc) referenced in the tableIndex.xml will also be processed.")
    .hasArgument(true).setOptionalArgument(false).required(true);

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
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterMap = new HashMap<String, Parameter>();

    parameterMap.put(folder.longName(), folder);
    parameterMap.put(tableFilter.longName(), tableFilter);
    parameterMap.put(archiveIndex.longName(), archiveIndex);
    parameterMap.put(contextDocumentationIndex.longName(), contextDocumentationIndex);
    parameterMap.put(contextDocmentationFolder.longName(), contextDocmentationFolder);
    parameterMap.put(PARAM_IMPORT_AS_SCHEMA.longName(), PARAM_IMPORT_AS_SCHEMA);
    parameterMap.put(PARAM_IMPORT_FOLDER.longName(), PARAM_IMPORT_FOLDER);
    // to be used later...
    // parameterMap.put(clobType.longName(), clobType);
    // parameterMap.put(clobLength.longName(), clobLength);

    return parameterMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(PARAM_IMPORT_FOLDER, PARAM_IMPORT_AS_SCHEMA), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    // return new Parameters(Arrays.asList(folder, archiveIndex,
    // contextDocumentationIndex, contextDocmentationFolder,
    // clobType, clobLength), null);

    return new Parameters(Arrays.asList(folder, tableFilter, archiveIndex, contextDocumentationIndex,
      contextDocmentationFolder), null);

  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) {
    Reporter.importModuleParameters(getModuleName(), "file", Paths.get(parameters.get(PARAM_IMPORT_FOLDER)).normalize()
      .toAbsolutePath().toString(), PARAM_IMPORT_AS_SCHEMA.longName(), parameters.get(PARAM_IMPORT_AS_SCHEMA));
    return new SIARDDKImportModule(Paths.get(parameters.get(PARAM_IMPORT_FOLDER)),
      parameters.get(PARAM_IMPORT_AS_SCHEMA)).getDatabaseImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {

    // Get the values passed to the parameter flags from the command line

    String pFolder = parameters.get(folder);
    String pArchiveIndex = parameters.get(archiveIndex);
    String pContextDocumentationIndex = parameters.get(contextDocumentationIndex);
    String pContextDocumentationFolder = parameters.get(contextDocmentationFolder);

    // optional
    Path pTableFilter = null;
    if (StringUtils.isNotBlank(parameters.get(tableFilter))) {
      pTableFilter = Paths.get(parameters.get(tableFilter));
    }

    // to be used later...
    // String pClobType = parameters.get(clobType);
    // String pClobLength = parameters.get(clobLength);

    Map<String, String> exportModuleArgs = new HashMap<String, String>();
    exportModuleArgs.put(folder.longName(), pFolder);
    // exportModuleArgs.put(tableFilter.longName(), pTableFilter.toString());
    exportModuleArgs.put(archiveIndex.longName(), pArchiveIndex);
    exportModuleArgs.put(contextDocumentationIndex.longName(), pContextDocumentationIndex);
    exportModuleArgs.put(contextDocmentationFolder.longName(), pContextDocumentationFolder);

    // to be used later...
    // exportModuleArgs.put(clobType.longName(), pClobType);
    // exportModuleArgs.put(clobLength.longName(), pClobLength);

    if(pTableFilter == null) {
      Reporter
        .exportModuleParameters(getModuleName(), folder.longName(), pFolder, archiveIndex.longName(), pArchiveIndex,
          contextDocumentationIndex.longName(), pContextDocumentationIndex, contextDocmentationFolder.longName(),
          pContextDocumentationFolder);
    }else{
      Reporter
        .exportModuleParameters(getModuleName(), folder.longName(), pFolder, archiveIndex.longName(), pArchiveIndex,
          contextDocumentationIndex.longName(), pContextDocumentationIndex, contextDocmentationFolder.longName(),
          pContextDocumentationFolder, tableFilter.longName(), pTableFilter.normalize().toAbsolutePath().toString());
    }

    return new SIARDDKExportModule(exportModuleArgs, pTableFilter).getDatabaseExportModule();
  }
}
