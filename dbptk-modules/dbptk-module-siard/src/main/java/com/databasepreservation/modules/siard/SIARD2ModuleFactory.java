/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ModuleFactory implements DatabaseModuleFactory {
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

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  // TODO: check if this argument is really necessary
  // private static final Parameter auxiliaryContainersInZipFormat = new
  // Parameter().shortName("...").longName("...").description(
  // "In some SIARD2 archives, LOBs are saved outside the main SIARD archive
  // container. These LOBs "+
  // "may be saved in a ZIP or simply saved to folders. When reading those LOBs
  // it's important "+
  // "to know if they are inside a simple folder or a zip
  // container.").hasArgument(false).required(false)
  // .valueIfNotSet("false").valueIfSet("true");

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
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  private static final Parameter metaArchiver = new Parameter().shortName("ma").longName(PARAMETER_META_ARCHIVER)
    .description("SIARD descriptive metadata field: Name of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  private static final Parameter metaArchiverContact = new Parameter().shortName("mac")
    .longName(PARAMETER_META_ARCHIVER_CONTACT)
    .description(
      "SIARD descriptive metadata field: Contact details (telephone, email) of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  private static final Parameter metaDataOwner = new Parameter().shortName("mdo").longName(PARAMETER_META_DATA_OWNER)
    .description(
      "SIARD descriptive metadata field: Owner of the data in the database. The person or institution that, at the time of archiving, has the right to grant usage rights for the data and is responsible for compliance with legal obligations such as data protection guidelines.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  private static final Parameter metaDataOriginTimespan = new Parameter().shortName("mdot")
    .longName(PARAMETER_META_DATA_ORIGIN_TIMESPAN)
    .description(
      "SIARD descriptive metadata field: Origination period of the data in the database (approximate indication in text form).")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

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
    return "siard-2";
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

    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(file), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(file, compress, prettyPrintXML, tableFilter, externalLobs,
      externalLobsPerFolder, externalLobsFolderSize, metaDescription, metaArchiver, metaArchiverContact, metaDataOwner,
      metaDataOriginTimespan, metaClientMachine), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return new SIARD2ImportModule(pFile).getDatabaseImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

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

    if (pExternalLobs) {
      if (pTableFilter == null) {
        reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
          PARAMETER_COMPRESS, String.valueOf(pCompress), PARAMETER_PRETTY_XML, String.valueOf(pPrettyPrintXML),
          PARAMETER_EXTERNAL_LOBS_PER_FOLDER, String.valueOf(pExternalLobsPerFolder),
          PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE, String.valueOf(pExternalLobsFolderSize));
      } else {
        reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
          PARAMETER_COMPRESS, String.valueOf(pCompress), PARAMETER_PRETTY_XML, String.valueOf(pPrettyPrintXML),
          PARAMETER_TABLE_FILTER, pTableFilter.normalize().toAbsolutePath().toString(),
          PARAMETER_EXTERNAL_LOBS_PER_FOLDER, String.valueOf(pExternalLobsPerFolder),
          PARAMETER_EXTERNAL_LOBS_FOLDER_SIZE, String.valueOf(pExternalLobsFolderSize));
      }

      return new SIARD2ExportModule(pFile, pCompress, pPrettyPrintXML, pTableFilter, pExternalLobsPerFolder,
        pExternalLobsFolderSize, descriptiveMetadataParameterValues).getDatabaseHandler();
    } else {
      if (pTableFilter == null) {
        reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
          PARAMETER_COMPRESS, String.valueOf(pCompress), PARAMETER_PRETTY_XML, String.valueOf(pPrettyPrintXML));
      } else {
        reporter.exportModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
          PARAMETER_COMPRESS, String.valueOf(pCompress), PARAMETER_PRETTY_XML, String.valueOf(pPrettyPrintXML),
          PARAMETER_TABLE_FILTER, pTableFilter.normalize().toAbsolutePath().toString());
      }

      return new SIARD2ExportModule(pFile, pCompress, pPrettyPrintXML, pTableFilter, descriptiveMetadataParameterValues)
        .getDatabaseHandler();
    }
  }

  private void addDescriptiveMetadataParameterValue(Map<Parameter, String> parameters,
    HashMap<String, String> descriptiveMetadataParameterValues, String description, Parameter metaDescription) {
    descriptiveMetadataParameterValues.put(description, parameters.get(metaDescription));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get(description))) {
      descriptiveMetadataParameterValues.put(description, metaDescription.valueIfNotSet());
    }
  }
}
