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
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.GMLExtractorFilter;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_VERSION = "version";
  public static final String PARAMETER_VERSION_2_0 = SIARDConstants.SiardVersion.V2_0.getDisplayName();
  public static final String PARAMETER_VERSION_2_1 = SIARDConstants.SiardVersion.V2_1.getDisplayName();
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_COMPRESS = "compress";
  public static final String PARAMETER_PRETTY_XML = "pretty-xml";
  public static final String PARAMETER_TABLE_FILTER = "table-filter";
  public static final String PARAMETER_EXTERNAL_LOBS = "external-lobs";
  public static final String PARAMETER_EXTERNAL_LOBS_PER_FOLDER = "external-lobs-per-folder";
  public static final String PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE = "external-lobs-folder-size";
  public static final String PARAMETER_META_DESCRIPTION = "meta-description";
  public static final String PARAMETER_META_ARCHIVER = "meta-archiver";
  public static final String PARAMETER_META_ARCHIVER_CONTACT = "meta-archiver-contact";
  public static final String PARAMETER_META_DATA_OWNER = "meta-data-owner";
  public static final String PARAMETER_META_DATA_ORIGIN_TIMESPAN = "meta-data-origin-timespan";
  public static final String PARAMETER_META_CLIENT_MACHINE = "meta-client-machine";
  public static final String PARAMETER_GML_DIRECTORY = "gml-directory";
  public static final String PARAMETER_VALIDATE = "validate";

  // humanized list of supported SIARD 2 versions
  private static final String versionsString = PARAMETER_VERSION_2_0 + " or " + PARAMETER_VERSION_2_1;

  private static final Parameter version = new Parameter().shortName("v").longName(PARAMETER_VERSION)
    .description("Choose SIARD version (" + versionsString + "). Default: latest (" + PARAMETER_VERSION_2_1 + ")")
    .hasArgument(true).required(false).setOptionalArgument(false).valueIfNotSet(PARAMETER_VERSION_2_1);

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter compress = new Parameter().shortName("c").longName(PARAMETER_COMPRESS)
    .description("use to compress the SIARD2 archive file with deflate method").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter prettyPrintXML = new Parameter().shortName("p").longName(PARAMETER_PRETTY_XML)
    .description("write human-readable XML").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  private static final Parameter tableFilter = new Parameter().shortName("tf").longName(PARAMETER_TABLE_FILTER)
    .description(
      "file with the list of tables that should be exported (this file can be created by the list-tables export module).")
    .required(false).hasArgument(true).setOptionalArgument(false);

  private static final Parameter externalLobs = new Parameter().shortName("el").longName(PARAMETER_EXTERNAL_LOBS)
    .description("Saves any LOBs outside the siard file.").required(false).hasArgument(false).valueIfSet("true")
    .valueIfNotSet("false");

  private static final Parameter externalLobsPerFolder = new Parameter().shortName("elpf")
    .longName(PARAMETER_EXTERNAL_LOBS_PER_FOLDER)
    .description("The maximum number of files present in an external LOB folder. Default: 1000 files.").required(false)
    .hasArgument(true).setOptionalArgument(false).valueIfNotSet("1000");

  private static final Parameter externalLobsFolderSize = new Parameter().shortName("elfs")
    .longName(PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE)
    .description(
      "Divide LOBs across multiple external folders with (approximately) the specified maximum size (in Megabytes). Default: do not divide.")
    .required(false).hasArgument(true).setOptionalArgument(false).valueIfNotSet("0");

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

  private static final Parameter gmlDirectory = new Parameter().shortName("gml").longName(PARAMETER_GML_DIRECTORY)
    .description("directory in which to create .gml files from tables with geometry data").required(false)
    .hasArgument(true).setOptionalArgument(false);

  private static final Parameter validate = new Parameter().shortName("v").longName(PARAMETER_VALIDATE)
    .description("use to validate the SIARD2 archive file after exporting").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

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
    return "siard-2";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(version.longName(), version);
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(compress.longName(), compress);
    parameterHashMap.put(prettyPrintXML.longName(), prettyPrintXML);
    parameterHashMap.put(tableFilter.longName(), tableFilter);
    parameterHashMap.put(externalLobs.longName(), externalLobs);
    parameterHashMap.put(externalLobsPerFolder.longName(), externalLobsPerFolder);
    parameterHashMap.put(externalLobsFolderSize.longName(), externalLobsFolderSize);
    parameterHashMap.put(metaDescription.longName(), metaDescription);
    parameterHashMap.put(metaArchiver.longName(), metaArchiver);
    parameterHashMap.put(metaArchiverContact.longName(), metaArchiverContact);
    parameterHashMap.put(metaDataOwner.longName(), metaDataOwner);
    parameterHashMap.put(metaDataOriginTimespan.longName(), metaDataOriginTimespan);
    parameterHashMap.put(metaClientMachine.longName(), metaClientMachine);
    parameterHashMap.put(gmlDirectory.longName(), gmlDirectory);
    parameterHashMap.put(validate.longName(), validate);

    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(file), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(version, file, compress, prettyPrintXML, tableFilter, externalLobs,
      externalLobsPerFolder, externalLobsFolderSize, metaDescription, metaArchiver, metaArchiverContact, metaDataOwner,
      metaDataOriginTimespan, metaClientMachine, gmlDirectory, validate), Collections.<ParameterGroup> emptyList());
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return new SIARD2ImportModule(pFile).getDatabaseImportModule();
  }

  public SIARD2ImportModule buildSiardModule(Map<Parameter, String> parameters, Reporter reporter) {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return  new SIARD2ImportModule(pFile);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

    // optional group, defaulting to latest version
    SIARDConstants.SiardVersion pVersion = SIARDConstants.SiardVersion.fromString(version.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(version))) {
      pVersion = SIARDConstants.SiardVersion.fromString(parameters.get(version));
      if (pVersion == null) {
        throw new UnsupportedModuleException(
          "Version " + parameters.get(version) + " is not valid. Supported versions are: " + versionsString);
      }
    }

    // optional
    boolean pCompress = Boolean.parseBoolean(compress.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(compress))) {
      pCompress = Boolean.parseBoolean(compress.valueIfSet());
    }

    // optional
    boolean pPrettyPrintXML = Boolean.parseBoolean(prettyPrintXML.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(prettyPrintXML))) {
      pPrettyPrintXML = Boolean.parseBoolean(prettyPrintXML.valueIfSet());
    }

    // optional
    Path pTableFilter = null;
    if (StringUtils.isNotBlank(parameters.get(tableFilter))) {
      pTableFilter = Paths.get(parameters.get(tableFilter));
    }

    // optional
    boolean pExternalLobs = Boolean.parseBoolean(externalLobs.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(externalLobs))) {
      pExternalLobs = Boolean.parseBoolean(externalLobs.valueIfSet());
    }

    // optional
    int pExternalLobsPerFolder = Integer.parseInt(externalLobsPerFolder.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(externalLobsPerFolder))) {
      pExternalLobsPerFolder = Integer.parseInt(parameters.get(externalLobsPerFolder));
      if (pExternalLobsPerFolder <= 0) {
        pExternalLobsPerFolder = Integer.parseInt(externalLobsPerFolder.valueIfNotSet());
      }
    }

    // optional
    long pExternalLobsFolderSize = Long.parseLong(externalLobsFolderSize.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(externalLobsFolderSize))) {
      pExternalLobsFolderSize = Long.parseLong(parameters.get(externalLobsFolderSize));
      if (pExternalLobsFolderSize <= 0) {
        pExternalLobsFolderSize = Long.parseLong(externalLobsFolderSize.valueIfNotSet());
      }
    }

    // optional
    Path pGMLDirectory = null;
    if (StringUtils.isNotBlank(parameters.get(gmlDirectory))) {
      pGMLDirectory = Paths.get(parameters.get(gmlDirectory));
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

    report(reporter, getModuleName(), PARAMETER_VERSION, String.valueOf(pVersion), PARAMETER_FILE, pFile,
      PARAMETER_COMPRESS, String.valueOf(pCompress), PARAMETER_PRETTY_XML, String.valueOf(pPrettyPrintXML),
      PARAMETER_EXTERNAL_LOBS, String.valueOf(pExternalLobs), PARAMETER_EXTERNAL_LOBS_PER_FOLDER,
      String.valueOf(pExternalLobsPerFolder), PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE,
      String.valueOf(pExternalLobsFolderSize), PARAMETER_TABLE_FILTER, pTableFilter);

    SIARD2ExportModule exportModule;
    DatabaseExportModule handler;

    if (pExternalLobs) {
      exportModule = new SIARD2ExportModule(pVersion, pFile, pCompress, pPrettyPrintXML, pTableFilter,
        pExternalLobsPerFolder, pExternalLobsFolderSize, descriptiveMetadataParameterValues);
    } else {
      exportModule = new SIARD2ExportModule(pVersion, pFile, pCompress, pPrettyPrintXML, pTableFilter,
        descriptiveMetadataParameterValues);
    }

    if (StringUtils.isNotBlank(parameters.get(validate))) {
      exportModule.setValidate(Boolean.parseBoolean(validate.valueIfSet()));
    }

    handler = exportModule.getDatabaseHandler();

    try {
      if (pGMLDirectory != null) {
        handler = new GMLExtractorFilter(pGMLDirectory).migrateDatabaseTo(handler);
      }
    } catch (ModuleException e) {
      throw new UnsupportedModuleException(e);
    }

    return handler;
  }

  private void addDescriptiveMetadataParameterValue(Map<Parameter, String> parameters,
    HashMap<String, String> descriptiveMetadataParameterValues, String description, Parameter metaDescription) {
    descriptiveMetadataParameterValues.put(description, parameters.get(metaDescription));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get(description))) {
      descriptiveMetadataParameterValues.put(description, metaDescription.valueIfNotSet());
    }
  }

  private void report(Reporter reporter, String moduleName, String parameterVersion, String parameterVersionValue,
    String parameterFile, Path parameterFileValue, String parameterCompress, String parameterCompressValue,
    String parameterPrettyXml, String parameterExternalLobs, String parameterExternalLobsValue,
    String parameterPrettyXmlValue, String parameterExternalLobsPerFolder, String parameterExternalLobsPerFolderValue,
    String parameterExternalLobsFolderSize, String parameterExternalLobsFolderSizeValue, String parameterTableFilter,
    Path parameterTableFilterValue) {

    String parameterFileValueString = null;
    if (parameterFileValue != null) {
      parameterFileValueString = parameterFileValue.normalize().toAbsolutePath().toString();
    }

    String parameterTableFilterValueString = null;
    if (parameterTableFilterValue != null) {
      parameterTableFilterValueString = parameterTableFilterValue.normalize().toAbsolutePath().toString();
    }

    reporter.exportModuleParameters(moduleName, parameterVersion, parameterVersionValue, parameterFile,
      parameterFileValueString, parameterCompress, parameterCompressValue, parameterPrettyXml, parameterExternalLobs,
      parameterExternalLobsValue, parameterPrettyXmlValue, parameterExternalLobsPerFolder,
      parameterExternalLobsPerFolderValue, parameterExternalLobsFolderSize, parameterExternalLobsFolderSizeValue,
      parameterTableFilter, parameterTableFilterValueString);
  }
}
