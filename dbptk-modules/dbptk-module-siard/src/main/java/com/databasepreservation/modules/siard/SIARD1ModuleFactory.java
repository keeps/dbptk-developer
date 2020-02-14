/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import static com.databasepreservation.Constants.UNSPECIFIED_METADATA_VALUE;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.CATEGORY_TYPE;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.input.SIARD1ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD1ExportModule;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_COMPRESS = "compress";
  public static final String PARAMETER_PRETTY_XML = "pretty-xml";
  public static final String PARAMETER_META_DESCRIPTION = "meta-description";
  public static final String PARAMETER_META_ARCHIVER = "meta-archiver";
  public static final String PARAMETER_META_ARCHIVER_CONTACT = "meta-archiver-contact";
  public static final String PARAMETER_META_DATA_OWNER = "meta-data-owner";
  public static final String PARAMETER_META_DATA_ORIGIN_TIMESPAN = "meta-data-origin-timespan";
  public static final String PARAMETER_META_CLIENT_MACHINE = "meta-client-machine";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to SIARD1 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter compress = new Parameter().shortName("c").longName(PARAMETER_COMPRESS)
    .description("use to compress the SIARD1 archive file with deflate method").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter prettyPrintXML = new Parameter().shortName("p").longName(PARAMETER_PRETTY_XML)
    .description("write human-readable XML").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  private static final Parameter metaDescription = new Parameter().shortName("md").longName(PARAMETER_META_DESCRIPTION)
    .description("SIARD descriptive metadata field: Description of database meaning and content as a whole.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

  private static final Parameter metaArchiver = new Parameter().shortName("ma").longName(PARAMETER_META_ARCHIVER)
    .description("SIARD descriptive metadata field: Name of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

  private static final Parameter metaArchiverContact = new Parameter().shortName("mac")
    .longName(PARAMETER_META_ARCHIVER_CONTACT)
    .description(
      "SIARD descriptive metadata field: Contact details (telephone, email) of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

  private static final Parameter metaDataOwner = new Parameter().shortName("mdo").longName(PARAMETER_META_DATA_OWNER)
    .description(
      "SIARD descriptive metadata field: Owner of the data in the database. The person or institution that, at the time of archiving, has the right to grant usage rights for the data and is responsible for compliance with legal obligations such as data protection guidelines.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

  private static final Parameter metaDataOriginTimespan = new Parameter().shortName("mdot")
    .longName(PARAMETER_META_DATA_ORIGIN_TIMESPAN)
    .description(
      "SIARD descriptive metadata field: Origination period of the data in the database (approximate indication in text form).")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

  private static final Parameter metaClientMachine = new Parameter().shortName("mcm")
    .longName(PARAMETER_META_CLIENT_MACHINE)
    .description(
      "SIARD descriptive metadata field: DNS name of the (client) computer on which the archiving was carried out.")
    .required(false).hasArgument(true).setOptionalArgument(true)
    .valueIfNotSet(SIARDHelper.getMachineHostname() + " (fetched automatically)");

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
    return "siard-1";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(compress.longName(), compress);
    parameterHashMap.put(prettyPrintXML.longName(), prettyPrintXML);
    parameterHashMap.put(metaDescription.longName(), metaDescription);
    parameterHashMap.put(metaArchiver.longName(), metaArchiver);
    parameterHashMap.put(metaArchiverContact.longName(), metaArchiverContact);
    parameterHashMap.put(metaDataOwner.longName(), metaDataOwner);
    parameterHashMap.put(metaDataOriginTimespan.longName(), metaDataOriginTimespan);
    parameterHashMap.put(metaClientMachine.longName(), metaClientMachine);
    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(Collections.singletonList(file.inputType(INPUT_TYPE.FILE_OPEN)), null);
  }

  @Override
  public Parameters getImportModuleParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(Arrays.asList(
      file.inputType(INPUT_TYPE.FILE_SAVE).fileFilter(Parameter.FILE_FILTER_TYPE.SIARD_EXTENSION)
        .exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      compress.inputType(INPUT_TYPE.CHECKBOX).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      prettyPrintXML.inputType(INPUT_TYPE.DEFAULT),
      metaDescription.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaArchiver.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaArchiverContact.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaDataOwner.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaDataOriginTimespan.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaClientMachine.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS)), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return new SIARD1ImportModule(pFile).getDatabaseImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter) {
    Path pFile = Paths.get(parameters.get(file));

    // optional
    boolean pCompress = Boolean.parseBoolean(compress.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(compress))) {
      pCompress = Boolean.parseBoolean(compress.valueIfSet());
    }

    boolean pPrettyPrintXML = Boolean.parseBoolean(prettyPrintXML.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(prettyPrintXML))) {
      pPrettyPrintXML = Boolean.parseBoolean(prettyPrintXML.valueIfSet());
    }

    // build descriptive metadata
    HashMap<String, String> descriptiveMetadataParameterValues = new HashMap<>();
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_DESCRIPTION, metaDescription);
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_ARCHIVER, metaArchiver);
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_ARCHIVER_CONTACT, metaArchiverContact);
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_DATA_OWNER, metaDataOwner);
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_DATA_ORIGIN_TIMESPAN, metaDataOriginTimespan);
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
      SIARDConstants.DESCRIPTIVE_METADATA_CLIENT_MACHINE, metaClientMachine);

    SIARD1ExportModule exportModule = new SIARD1ExportModule(pFile, pCompress, pPrettyPrintXML,
      descriptiveMetadataParameterValues);

    return exportModule.getDatabaseHandler();
  }

  private void addDescriptiveMetadataParameterValue(Map<Parameter, String> parameters,
    HashMap<String, String> descriptiveMetadataParameterValues, String description, Parameter metaDescription) {
    descriptiveMetadataParameterValues.put(description, parameters.get(metaDescription));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get(description))) {
      descriptiveMetadataParameterValues.put(description, metaDescription.valueIfNotSet());
    }
  }
}
