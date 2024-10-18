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

import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007FileIndexFileStrategy;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007ContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK1007DatabaseExportModule extends SIARDExportDefault {

  private SIARDDK1007ExportModule siarddk1007ExportModule;
  private static final Logger logger = LoggerFactory.getLogger(SIARDDK1007DatabaseExportModule.class);

  public SIARDDK1007DatabaseExportModule(SIARDDK1007ExportModule siarddk1007ExportModule) {
    super(siarddk1007ExportModule.getContentExportStrategy(), siarddk1007ExportModule.getMainContainer(),
      siarddk1007ExportModule.getWriteStrategy(), siarddk1007ExportModule.getMetadataExportStrategy(), null);

    this.siarddk1007ExportModule = siarddk1007ExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    // Get docID info from the command line and add these to the LOBsTracker

    Path pathToArchive = siarddk1007ExportModule.getMainContainer().getPath();

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

    Map<String, String> exportModuleArgs = siarddk1007ExportModule.getExportModuleArgs();
    SIARDDK1007FileIndexFileStrategy SIARDDK1007FileIndexFileStrategy = siarddk1007ExportModule.getFileIndexFileStrategy();
    MetadataPathStrategy metadataPathStrategy = siarddk1007ExportModule.getMetadataPathStrategy();
    SIARDMarshaller siardMarshaller = siarddk1007ExportModule.getSiardMarshaller();

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      SIARDDK1007ContextDocumentationWriter SIARDDK1007ContextDocumentationWriter = new SIARDDK1007ContextDocumentationWriter(
        siarddk1007ExportModule.getMainContainer(), siarddk1007ExportModule.getWriteStrategy(), SIARDDK1007FileIndexFileStrategy,
        siarddk1007ExportModule.getExportModuleArgs());

      SIARDDK1007ContextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    // TO-DO: refactor the stuff below into separate class (also to be used by
    // the MetadataExportStrategy)

    try {
      SIARDDK1007FileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException().withMessage("Error writing fileIndex.xml").withCause(e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX);
      OutputStream writer = SIARDDK1007FileIndexFileStrategy.getWriter(siarddk1007ExportModule.getMainContainer(), path,
        siarddk1007ExportModule.getWriteStrategy());

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_FILEINDEX,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        SIARDDK1007FileIndexFileStrategy.generateXML(null));

      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing fileIndex to the archive.").withCause(e);
    }

  }
}
