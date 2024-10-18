/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK128ContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.SIARDDK128FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDK128DatabaseExportModule extends SIARDExportDefault {

  private SIARDDK128ExportModule siarddk128ExportModule;
  private static final Logger logger = LoggerFactory.getLogger(SIARDDK128DatabaseExportModule.class);

  public SIARDDK128DatabaseExportModule(SIARDDK128ExportModule siarddk128ExportModule) {
    super(siarddk128ExportModule.getContentExportStrategy(), siarddk128ExportModule.getMainContainer(),
      siarddk128ExportModule.getWriteStrategy(), siarddk128ExportModule.getMetadataExportStrategy(), null);

    this.siarddk128ExportModule = siarddk128ExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    // Get docID info from the command line and add these to the LOBsTracker

    Path pathToArchive = siarddk128ExportModule.getMainContainer().getPath();

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

    Map<String, String> exportModuleArgs = siarddk128ExportModule.getExportModuleArgs();
    SIARDDK128FileIndexFileStrategy SIARDDK128FileIndexFileStrategy = siarddk128ExportModule.getFileIndexFileStrategy();
    MetadataPathStrategy metadataPathStrategy = siarddk128ExportModule.getMetadataPathStrategy();
    SIARDMarshaller siardMarshaller = siarddk128ExportModule.getSiardMarshaller();

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      SIARDDK128ContextDocumentationWriter SIARDDK128ContextDocumentationWriter = new SIARDDK128ContextDocumentationWriter(
        siarddk128ExportModule.getMainContainer(), siarddk128ExportModule.getWriteStrategy(), SIARDDK128FileIndexFileStrategy,
        siarddk128ExportModule.getExportModuleArgs());

      SIARDDK128ContextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    // TO-DO: refactor the stuff below into separate class (also to be used by
    // the MetadataExportStrategy)

    try {
      SIARDDK128FileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException().withMessage("Error writing fileIndex.xml").withCause(e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX);
      OutputStream writer = SIARDDK128FileIndexFileStrategy.getWriter(siarddk128ExportModule.getMainContainer(), path,
        siarddk128ExportModule.getWriteStrategy());

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_128,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        SIARDDK128FileIndexFileStrategy.generateXML(null));

      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing fileIndex to the archive.").withCause(e);
    }

  }
}
