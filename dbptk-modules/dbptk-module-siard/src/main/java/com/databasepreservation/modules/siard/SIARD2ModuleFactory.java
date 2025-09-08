/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import static com.databasepreservation.Constants.UNSPECIFIED_METADATA_VALUE;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameter.CATEGORY_TYPE;
import com.databasepreservation.model.parameters.Parameter.INPUT_TYPE;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.GMLExtractorFilter;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;
import com.databasepreservation.utils.ModuleUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_VERSION = "version";
  public static final String PARAMETER_VERSION_2_0 = SIARDConstants.SiardVersion.V2_0.getDisplayName();
  public static final String PARAMETER_VERSION_2_1 = SIARDConstants.SiardVersion.V2_1.getDisplayName();
  public static final String PARAMETER_VERSION_2_2 = SIARDConstants.SiardVersion.V2_2.getDisplayName();
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_COMPRESS = "compress";
  public static final String PARAMETER_PRETTY_XML = "pretty-xml";
  public static final String PARAMETER_EXTERNAL_LOBS = "external-lobs";
  public static final String PARAMETER_EXTERNAL_LOBS_PER_FOLDER = "external-lobs-per-folder";
  public static final String PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE = "external-lobs-folder-size";
  public static final String PARAMETER_EXTERNAL_LOBS_BLOB_THRESHOLD_LIMIT = "external-lobs-blob-threshold-limit";
  public static final String PARAMETER_EXTERNAL_LOBS_CLOB_THRESHOLD_LIMIT = "external-lobs-clob-threshold-limit";
  public static final String PARAMETER_META_DBNAME = "meta-dbname";
  public static final String PARAMETER_META_DESCRIPTION = "meta-description";
  public static final String PARAMETER_META_ARCHIVER = "meta-archiver";
  public static final String PARAMETER_META_ARCHIVER_CONTACT = "meta-archiver-contact";
  public static final String PARAMETER_META_DATA_OWNER = "meta-data-owner";
  public static final String PARAMETER_META_DATA_ORIGIN_TIMESPAN = "meta-data-origin-timespan";
  public static final String PARAMETER_META_CLIENT_MACHINE = "meta-client-machine";
  public static final String PARAMETER_GML_DIRECTORY = "gml-directory";
  public static final String PARAMETER_MESSAGE_DIGEST_ALGORITHM = "digest";
  public static final String PARAMETER_FONT_CASE = "font-case";
  public static final String PARAMETER_IGNORE_LOBS = "ignore-lobs";

  // humanized list of supported SIARD 2 versions
  private static final String versionsString = PARAMETER_VERSION_2_0 + " or " + PARAMETER_VERSION_2_1 + " or "
    + PARAMETER_VERSION_2_2;

  private static final Parameter version = new Parameter().shortName("v").longName(PARAMETER_VERSION)
    .description("Choose SIARD version (" + versionsString + "). Default: latest (" + PARAMETER_VERSION_2_2 + ")")
    .hasArgument(true).required(false).setOptionalArgument(false).valueIfNotSet(PARAMETER_VERSION_2_2);

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter compress = new Parameter().shortName("c").longName(PARAMETER_COMPRESS)
    .description("use to compress the SIARD2 archive file with deflate method").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter prettyPrintXML = new Parameter().shortName("p").longName(PARAMETER_PRETTY_XML)
    .description("write human-readable XML").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  private static final Parameter externalLobs = new Parameter().shortName("el").longName(PARAMETER_EXTERNAL_LOBS)
    .description("Saves any LOBs outside the SIARD file.").required(false).hasArgument(false).valueIfSet("true")
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

  private static final Parameter externalLobsBLOBThresholdLimit = new Parameter().shortName("elblobtl")
    .longName(PARAMETER_EXTERNAL_LOBS_BLOB_THRESHOLD_LIMIT)
    .description(
      "Keep BLOBs stored inside the SIARD file if the threshold is not exceeded (in bytes). Default: 2000 bytes.")
    .required(false).hasArgument(true).setOptionalArgument(false).valueIfNotSet("2000");

  private static final Parameter externalLobsCLOBThresholdLimit = new Parameter().shortName("elclobtl")
    .longName(PARAMETER_EXTERNAL_LOBS_CLOB_THRESHOLD_LIMIT)
    .description(
      "Keep CLOBs stored inside the SIARD file if the threshold is not exceeded (in bytes). Default: 4000 bytes.")
    .required(false).hasArgument(true).setOptionalArgument(false).valueIfNotSet("4000");

  private static final Parameter metaDbname = new Parameter().shortName("mdb").longName(PARAMETER_META_DBNAME)
    .description("SIARD descriptive metadata field: Short database identifier").required(false).hasArgument(true)
    .setOptionalArgument(true).valueIfNotSet(UNSPECIFIED_METADATA_VALUE);

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

  private static final Parameter messageDigestAlgorithm = new Parameter().shortName("d")
    .longName(PARAMETER_MESSAGE_DIGEST_ALGORITHM)
    .description("The message digest algorithm for the type of integrity information (Default: SHA-256)")
    .hasArgument(true).setOptionalArgument(false).required(false).valueIfNotSet("SHA-256");

  private static final Parameter fontCase = new Parameter().shortName("fc").longName(PARAMETER_FONT_CASE).description(
    "Define the type of font case for the message digest. Supported font case are: upper case and lower case. (Default: lowercase)")
    .hasArgument(true).required(false).valueIfNotSet("lowercase");

  // For DBPTK Enterprise use; please don't use this parameter very dangerous
  private static final Parameter ignoreLobs = new Parameter().shortName("ignl").longName(PARAMETER_IGNORE_LOBS)
    .description(
      "Ignores the LOBs by not reading them from the SIARD and ultimately not being available when importing a SIARD")
    .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true").showOnHelpMenu(false);

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
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(version.longName(), version);
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(compress.longName(), compress);
    parameterHashMap.put(prettyPrintXML.longName(), prettyPrintXML);
    parameterHashMap.put(externalLobs.longName(), externalLobs);
    parameterHashMap.put(externalLobsPerFolder.longName(), externalLobsPerFolder);
    parameterHashMap.put(externalLobsFolderSize.longName(), externalLobsFolderSize);
    parameterHashMap.put(externalLobsBLOBThresholdLimit.longName(), externalLobsBLOBThresholdLimit);
    parameterHashMap.put(externalLobsCLOBThresholdLimit.longName(), externalLobsCLOBThresholdLimit);
    parameterHashMap.put(metaDbname.longName(), metaDbname);
    parameterHashMap.put(metaDescription.longName(), metaDescription);
    parameterHashMap.put(metaArchiver.longName(), metaArchiver);
    parameterHashMap.put(metaArchiverContact.longName(), metaArchiverContact);
    parameterHashMap.put(metaDataOwner.longName(), metaDataOwner);
    parameterHashMap.put(metaDataOriginTimespan.longName(), metaDataOriginTimespan);
    parameterHashMap.put(metaClientMachine.longName(), metaClientMachine);
    parameterHashMap.put(gmlDirectory.longName(), gmlDirectory);
    parameterHashMap.put(messageDigestAlgorithm.longName(), messageDigestAlgorithm);
    parameterHashMap.put(fontCase.longName(), fontCase);
    parameterHashMap.put(ignoreLobs.longName(), ignoreLobs);

    return parameterHashMap;
  }

  @Override
  public Parameters getConnectionParameters() {
    return new Parameters(Collections.singletonList(file.inputType(INPUT_TYPE.FILE_OPEN)), null);
  }

  @Override
  public Parameters getImportModuleParameters() {
    return new Parameters(Arrays.asList(file, ignoreLobs), null);
  }

  @Override
  public Parameters getExportModuleParameters() {
    return new Parameters(Arrays.asList(version.inputType(INPUT_TYPE.NONE).exportOptions(CATEGORY_TYPE.NONE),
      file.inputType(INPUT_TYPE.FILE_SAVE).fileFilter(Parameter.FILE_FILTER_TYPE.SIARD_EXTENSION)
        .exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      compress.inputType(INPUT_TYPE.CHECKBOX).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      prettyPrintXML.inputType(INPUT_TYPE.CHECKBOX).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      externalLobs.inputType(INPUT_TYPE.CHECKBOX).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
      externalLobsPerFolder.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
      externalLobsFolderSize.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
      externalLobsBLOBThresholdLimit.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
      externalLobsCLOBThresholdLimit.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.EXTERNAL_LOBS),
      metaDbname.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaDescription.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaArchiver.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaArchiverContact.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaDataOwner.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaDataOriginTimespan.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      metaClientMachine.inputType(INPUT_TYPE.TEXT).exportOptions(CATEGORY_TYPE.METADATA_EXPORT_OPTIONS),
      gmlDirectory.inputType(INPUT_TYPE.DEFAULT),
      messageDigestAlgorithm.inputType(INPUT_TYPE.COMBOBOX).possibleValues("MD5", "SHA-1", "SHA-256")
        .defaultSelectedIndex(2).exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS),
      fontCase.inputType(INPUT_TYPE.COMBOBOX).possibleValues("uppercase", "lowercase").defaultSelectedIndex(1)
        .exportOptions(CATEGORY_TYPE.SIARD_EXPORT_OPTIONS)),
      Collections.emptyList());
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));

    boolean pIgnoreLobs = Boolean.parseBoolean(parameters.get(ignoreLobs));

    if (Files.notExists(pFile)) {
      throw new SiardNotFoundException().withPath(pFile.toAbsolutePath().toString())
        .withMessage("The path to the siard file appears to be incorrect");
    }

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
      PARAMETER_IGNORE_LOBS, Boolean.toString(pIgnoreLobs));
    return new SIARD2ImportModule(pFile, pIgnoreLobs).getDatabaseImportModule();
  }

  @Override
  public DatabaseFilterModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
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
    long pExternalLobsBLOBThresholdLimit = Long.parseLong(externalLobsBLOBThresholdLimit.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(externalLobsBLOBThresholdLimit))) {
      pExternalLobsBLOBThresholdLimit = Long.parseLong(parameters.get(externalLobsBLOBThresholdLimit));
      if (pExternalLobsBLOBThresholdLimit < 0) {
        pExternalLobsBLOBThresholdLimit = Long.parseLong(externalLobsBLOBThresholdLimit.valueIfNotSet());
      }
    }

    // optional
    long pExternalLobsCLOBThresholdLimit = Long.parseLong(externalLobsCLOBThresholdLimit.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(externalLobsCLOBThresholdLimit))) {
      pExternalLobsCLOBThresholdLimit = Long.parseLong(parameters.get(externalLobsCLOBThresholdLimit));
      if (pExternalLobsCLOBThresholdLimit < 0) {
        pExternalLobsCLOBThresholdLimit = Long.parseLong(externalLobsCLOBThresholdLimit.valueIfNotSet());
      }
    }

    // optional
    Path pGMLDirectory = null;
    if (StringUtils.isNotBlank(parameters.get(gmlDirectory))) {
      pGMLDirectory = Paths.get(parameters.get(gmlDirectory));
    }

    // digest algorithm
    String pDigestAlgorithm;
    if (StringUtils.isNotBlank(parameters.get(messageDigestAlgorithm))) {
      pDigestAlgorithm = parameters.get(messageDigestAlgorithm);
      try {
        MessageDigest.getInstance(pDigestAlgorithm);
      } catch (NoSuchAlgorithmException e) {
        throw new ModuleException()
          .withMessage("The message digest algorithm '" + pDigestAlgorithm + "' does not exits").withCause(e);
      }
    } else {
      pDigestAlgorithm = messageDigestAlgorithm.valueIfNotSet();
    }

    // font case
    String pFontCase = fontCase.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(fontCase))) {
      pFontCase = parameters.get(fontCase);
    }
    ModuleUtils.validateFontCase(pFontCase);

    // build descriptive metadata
    HashMap<String, String> descriptiveMetadataParameterValues = new HashMap<>();
    addDescriptiveMetadataParameterValue(parameters, descriptiveMetadataParameterValues,
        SIARDConstants.DESCRIPTIVE_METADATA_DBNAME, metaDbname);
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

    report(reporter, getModuleName(), String.valueOf(pVersion), pFile, String.valueOf(pCompress),
      String.valueOf(pPrettyPrintXML), String.valueOf(pExternalLobs), String.valueOf(pExternalLobsPerFolder),
      String.valueOf(pExternalLobsFolderSize), String.valueOf(pExternalLobsBLOBThresholdLimit),
      String.valueOf(pExternalLobsCLOBThresholdLimit), pDigestAlgorithm, pFontCase);

    SIARD2ExportModule exportModule;
    DatabaseFilterModule handler;

    if (pExternalLobs) {
      exportModule = new SIARD2ExportModule(pVersion, pFile, pCompress, pPrettyPrintXML, pExternalLobsPerFolder,
        pExternalLobsFolderSize, pExternalLobsBLOBThresholdLimit, pExternalLobsCLOBThresholdLimit,
        descriptiveMetadataParameterValues, pDigestAlgorithm, pFontCase);
    } else {
      exportModule = new SIARD2ExportModule(pVersion, pFile, pCompress, pPrettyPrintXML,
        descriptiveMetadataParameterValues, pDigestAlgorithm, pFontCase);
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

  private void report(Reporter reporter, String moduleName, String parameterVersionValue, Path parameterFileValue,
    String parameterCompressValue, String parameterPrettyXmlValue, String parameterExternalLobsValue,
    String parameterExternalLobsPerFolderValue, String parameterExternalLobsFolderSizeValue,
    String parameterExternalLobsBLOBThresholdLimit, String parameterExternalLobsCLOBThresholdLimit,
    String parameterMessageDigestAlgorithmValue, String parameterFontCaseValue) {

    String parameterFileValueString = null;
    if (parameterFileValue != null) {
      parameterFileValueString = parameterFileValue.normalize().toAbsolutePath().toString();
    }

    reporter.exportModuleParameters(moduleName, PARAMETER_VERSION, parameterVersionValue, PARAMETER_FILE,
      parameterFileValueString, PARAMETER_COMPRESS, parameterCompressValue, PARAMETER_PRETTY_XML,
      parameterPrettyXmlValue, PARAMETER_EXTERNAL_LOBS, parameterExternalLobsValue, PARAMETER_EXTERNAL_LOBS_PER_FOLDER,
      parameterExternalLobsPerFolderValue, PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE, parameterExternalLobsFolderSizeValue,
      parameterExternalLobsBLOBThresholdLimit, PARAMETER_EXTERNAL_LOBS_BLOB_THRESHOLD_LIMIT,
      parameterExternalLobsCLOBThresholdLimit, PARAMETER_EXTERNAL_LOBS_CLOB_THRESHOLD_LIMIT,
      PARAMETER_MESSAGE_DIGEST_ALGORITHM, parameterMessageDigestAlgorithmValue, PARAMETER_FONT_CASE,
      parameterFontCaseValue);
  }
}
