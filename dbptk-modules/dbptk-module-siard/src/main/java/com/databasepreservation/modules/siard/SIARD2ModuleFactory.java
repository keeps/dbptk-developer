package com.databasepreservation.modules.siard;

import java.nio.file.Path;
import java.nio.file.Paths;
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
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ModuleFactory implements DatabaseModuleFactory {
  private static final Parameter file = new Parameter().shortName("f").longName("file")
    .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  // TODO: check if this argument is really necessary
  // private static final Parameter auxiliaryContainersInZipFormat = new
  // Parameter().shortName("...").longName("...").description(
  // "In some SIARD2 archives, LOBs are saved outside the main SIARD archive container. These LOBs "+
  // "may be saved in a ZIP or simply saved to folders. When reading those LOBs it's important "+
  // "to know if they are inside a simple folder or a zip container.").hasArgument(false).required(false)
  // .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter compress = new Parameter().shortName("c").longName("compress")
    .description("use to compress the SIARD2 archive file with deflate method").hasArgument(false).required(false)
    .valueIfNotSet("false").valueIfSet("true");

  private static final Parameter prettyPrintXML = new Parameter().shortName("p").longName("pretty-xml")
    .description("write human-readable XML").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  private static final Parameter tableFilter = new Parameter()
    .shortName("tf")
    .longName("table-filter")
    .description(
      "file with the list of tables that should be exported (this file can be created by the list-tables export module).")
    .required(false).hasArgument(true).setOptionalArgument(false);

  private static final Parameter externalLobs = new Parameter().shortName("el").longName("external-lobs")
    .description("Saves any LOBs outside the siard file.").required(false).hasArgument(false).valueIfSet("true")
    .valueIfNotSet("false");

  private static final Parameter externalLobsPerFolder = new Parameter().shortName("elpf")
    .longName("external-lobs-per-folder")
    .description("The maximum number of files present in an external LOB folder. Default: 1000 files.").required(false)
    .hasArgument(true).setOptionalArgument(false).valueIfNotSet("1000");

  private static final Parameter externalLobsFolderSize = new Parameter()
    .shortName("elfs")
    .longName("external-lobs-folder-size")
    .description(
      "Divide LOBs across multiple external folders with (approximately) the specified maximum size (in Megabytes). Default: do not divide.")
    .required(false).hasArgument(true).setOptionalArgument(false).valueIfNotSet("0");

  public static final Parameter metaDescription = new Parameter().shortName("md").longName("meta-description")
    .description("SIARD descriptive metadata field: Description of database meaning and content as a whole.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  public static final Parameter metaArchiver = new Parameter().shortName("ma").longName("meta-archiver")
    .description("SIARD descriptive metadata field: Name of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  public static final Parameter metaArchiverContact = new Parameter()
    .shortName("mac")
    .longName("meta-archiver-contact")
    .description(
      "SIARD descriptive metadata field: Contact details (telephone, email) of the person who carried out the archiving of the database.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  public static final Parameter metaDataOwner = new Parameter()
    .shortName("mdo")
    .longName("meta-data-owner")
    .description(
      "SIARD descriptive metadata field: Owner of the data in the database. The person or institution that, at the time of archiving, has the right to grant usage rights for the data and is responsible for compliance with legal obligations such as data protection guidelines.")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  public static final Parameter metaDataOriginTimespan = new Parameter()
    .shortName("mdot")
    .longName("meta-data-origin-timespan")
    .description(
      "SIARD descriptive metadata field: Origination period of the data in the database (approximate indication in text form).")
    .required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("unspecified");

  public static final Parameter metaClientMachine = new Parameter()
    .shortName("mcm")
    .longName("meta-client-machine")
    .description(
      "SIARD descriptive metadata field: DNS name of the (client) computer on which the archiving was carried out.")
    .required(false).hasArgument(true).setOptionalArgument(true)
    .valueIfNotSet(SIARDHelper.getMachineHostname() + " (fetched automatically)");

  private Reporter reporter;

  private SIARD2ModuleFactory() {
  }

  public SIARD2ModuleFactory(Reporter reporter) {
    this.reporter = reporter;
  }

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
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), "file", pFile.normalize().toAbsolutePath().toString());
    return new SIARD2ImportModule(pFile).getDatabaseImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
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

    // descriptive metadata
    List<Parameter> descriptiveMetadataParameters = Arrays.asList(metaDescription, metaArchiver, metaArchiverContact,
      metaDataOwner, metaDataOriginTimespan, metaClientMachine);
    HashMap<String, String> descriptiveMetadataParameterValues = new HashMap<>(descriptiveMetadataParameters.size());
    descriptiveMetadataParameterValues.put("Description", parameters.get(metaDescription));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("Description"))) {
      descriptiveMetadataParameterValues.put("Description", metaDescription.valueIfNotSet());
    }
    descriptiveMetadataParameterValues.put("Archiver", parameters.get(metaArchiver));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("Archiver"))) {
      descriptiveMetadataParameterValues.put("Archiver", metaArchiver.valueIfNotSet());
    }
    descriptiveMetadataParameterValues.put("ArchiverContact", parameters.get(metaArchiverContact));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("ArchiverContact"))) {
      descriptiveMetadataParameterValues.put("ArchiverContact", metaArchiverContact.valueIfNotSet());
    }
    descriptiveMetadataParameterValues.put("DataOwner", parameters.get(metaDataOwner));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("DataOwner"))) {
      descriptiveMetadataParameterValues.put("DataOwner", metaDataOwner.valueIfNotSet());
    }
    descriptiveMetadataParameterValues.put("DataOriginTimespan", parameters.get(metaDataOriginTimespan));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("DataOriginTimespan"))) {
      descriptiveMetadataParameterValues.put("DataOriginTimespan", metaDataOriginTimespan.valueIfNotSet());
    }
    descriptiveMetadataParameterValues.put("ClientMachine", parameters.get(metaClientMachine));
    if (StringUtils.isBlank(descriptiveMetadataParameterValues.get("ClientMachine"))) {
      descriptiveMetadataParameterValues.put("ClientMachine", metaClientMachine.valueIfNotSet());
    }

    if (pExternalLobs) {
      if (pTableFilter == null) {
        reporter.exportModuleParameters(getModuleName(), "file", pFile.normalize().toAbsolutePath().toString(),
          "compress", String.valueOf(pCompress), "pretty xml", String.valueOf(pPrettyPrintXML),
          "external lobs per folder", String.valueOf(pExternalLobsPerFolder), "external lobs folder size",
          String.valueOf(pExternalLobsFolderSize));
      } else {
        reporter.exportModuleParameters(getModuleName(), "file", pFile.normalize().toAbsolutePath().toString(),
          "compress", String.valueOf(pCompress), "pretty xml", String.valueOf(pPrettyPrintXML), "table filter",
          pTableFilter.normalize().toAbsolutePath().toString(), "external lobs per folder",
          String.valueOf(pExternalLobsPerFolder), "external lobs folder size", String.valueOf(pExternalLobsFolderSize));
      }

      return new SIARD2ExportModule(pFile, pCompress, pPrettyPrintXML, pTableFilter, pExternalLobsPerFolder,
        pExternalLobsFolderSize, descriptiveMetadataParameterValues).getDatabaseHandler();
    } else {
      if (pTableFilter == null) {
        reporter.exportModuleParameters(getModuleName(), "file", pFile.normalize().toAbsolutePath().toString(),
          "compress", String.valueOf(pCompress), "pretty xml", String.valueOf(pPrettyPrintXML));
      } else {
        reporter.exportModuleParameters(getModuleName(), "file", pFile.normalize().toAbsolutePath().toString(),
          "compress", String.valueOf(pCompress), "pretty xml", String.valueOf(pPrettyPrintXML), "table filter",
          pTableFilter.normalize().toAbsolutePath().toString());
      }

      return new SIARD2ExportModule(pFile, pCompress, pPrettyPrintXML, pTableFilter, descriptiveMetadataParameterValues)
        .getDatabaseHandler();
    }
  }
}
