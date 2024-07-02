/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import com.databasepreservation.modules.siard.out.metadata.SIARDDK2010FileIndexFileStrategy;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK2010ContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK2010DatabaseExportModule extends SIARDExportDefault {

  private SIARDDK2010ExportModule siarddk2010ExportModule;
  private static final Logger logger = LoggerFactory.getLogger(SIARDDK2010DatabaseExportModule.class);

  public SIARDDK2010DatabaseExportModule(SIARDDK2010ExportModule siarddk2010ExportModule) {
    super(siarddk2010ExportModule.getContentExportStrategy(), siarddk2010ExportModule.getMainContainer(),
      siarddk2010ExportModule.getWriteStrategy(), siarddk2010ExportModule.getMetadataExportStrategy(), null);

    this.siarddk2010ExportModule = siarddk2010ExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    // Get docID info from the command line and add these to the LOBsTracker

    Path pathToArchive = siarddk2010ExportModule.getMainContainer().getPath();

    // Check if the archive folder name is correct (must match
    // AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*)

    String regex = "AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.[1-9][0-9]*";
    String folderName = pathToArchive.getFileName().toString();
    if (!folderName.matches(regex)) {
      throw new ModuleException().withMessage("Archive folder name must match the expression " + regex);
    }

    // Backup output folder if it already exists

    File outputFolder = pathToArchive.toFile();

    if (outputFolder.isDirectory()) {
      try {

        // Get the creation time of the old archive folder
        BasicFileAttributes basicFileAttributes = Files.readAttributes(pathToArchive, BasicFileAttributes.class);
        String creationTimeStamp = basicFileAttributes.creationTime().toString();
        
        String name = pathToArchive.toString() + "_backup_" + creationTimeStamp;

        // Rename the old folder
        File oldArchiveDir = new File(name.replaceAll("[:\\\\/*?|<>]", "_"));
        FileUtils.moveDirectory(outputFolder, oldArchiveDir);

        logger.info("Backed up an already existing archive folder to: " + oldArchiveDir);
      } catch (IOException e) {
        throw new ModuleException().withMessage("Error deleting existing directory").withCause(e);
      }
    }
  }

  @Override
  public void finishDatabase() throws ModuleException {
    super.finishDatabase();

    // Write ContextDocumentation to archive

    Map<String, String> exportModuleArgs = siarddk2010ExportModule.getExportModuleArgs();
    SIARDDK2010FileIndexFileStrategy SIARDDK2010FileIndexFileStrategy = siarddk2010ExportModule.getFileIndexFileStrategy();
    MetadataPathStrategy metadataPathStrategy = siarddk2010ExportModule.getMetadataPathStrategy();
    SIARDMarshaller siardMarshaller = siarddk2010ExportModule.getSiardMarshaller();

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      SIARDDK2010ContextDocumentationWriter SIARDDK2010ContextDocumentationWriter = new SIARDDK2010ContextDocumentationWriter(
        siarddk2010ExportModule.getMainContainer(), siarddk2010ExportModule.getWriteStrategy(), SIARDDK2010FileIndexFileStrategy,
        siarddk2010ExportModule.getExportModuleArgs());

      SIARDDK2010ContextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    // TO-DO: refactor the stuff below into separate class (also to be used by
    // the MetadataExportStrategy)

    try {
      SIARDDK2010FileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException().withMessage("Error writing fileIndex.xml").withCause(e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX);
      OutputStream writer = SIARDDK2010FileIndexFileStrategy.getWriter(siarddk2010ExportModule.getMainContainer(), path,
        siarddk2010ExportModule.getWriteStrategy());

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_FILEINDEX,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        SIARDDK2010FileIndexFileStrategy.generateXML(null));

      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing fileIndex to the archive.").withCause(e);
    }

  }
}
