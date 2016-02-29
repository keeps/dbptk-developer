package com.databasepreservation.modules.siard.out.output;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.ContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKDatabaseExportModule extends SIARDExportDefault {

  private SIARDDKExportModule siarddkExportModule;
  private final CustomLogger logger = CustomLogger.getLogger(SIARDDKDatabaseExportModule.class);

  public SIARDDKDatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    super(siarddkExportModule.getContentExportStrategy(), siarddkExportModule.getMainContainer(), siarddkExportModule
      .getWriteStrategy(), siarddkExportModule.getMetadataExportStrategy(), null);

    this.siarddkExportModule = siarddkExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    // Get docID info from the command line and add these to the LOBsTracker

    Path pathToArchive = siarddkExportModule.getMainContainer().getPath();

    // Check if the archive folder name is correct (must match
    // AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*)

    String regex = "AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.[1-9][0-9]*";
    String folderName = pathToArchive.getFileName().toString();
    if (!folderName.matches(regex)) {
      throw new ModuleException("Archive folder name must match the expression " + regex);
    }

    // Backup output folder if it already exists

    File outputFolder = pathToArchive.toFile();

    if (outputFolder.isDirectory()) {
      try {

        // Get the creation time of the old archive folder
        BasicFileAttributes basicFileAttributes = Files.readAttributes(pathToArchive, BasicFileAttributes.class);
        String creationTimeStamp = basicFileAttributes.creationTime().toString();

        // Rename the old folder
        File oldArchiveDir = new File(pathToArchive.toString() + "_backup_" + creationTimeStamp);
        FileUtils.moveDirectory(outputFolder, oldArchiveDir);

        logger.info("Backed up an already existing archive folder to: " + oldArchiveDir);
      } catch (IOException e) {
        throw new ModuleException("Error deleting existing directory", e);
      }
    }
  }

  @Override
  public void finishDatabase() throws ModuleException {
    super.finishDatabase();

    // Write ContextDocumentation to archive

    Map<String, String> exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    FileIndexFileStrategy fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    MetadataPathStrategy metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    SIARDMarshaller siardMarshaller = siarddkExportModule.getSiardMarshaller();

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      ContextDocumentationWriter contextDocumentationWriter = new ContextDocumentationWriter(
        siarddkExportModule.getMainContainer(), siarddkExportModule.getWriteStrategy(), fileIndexFileStrategy,
        siarddkExportModule.getExportModuleArgs());

      contextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    // TO-DO: refactor the stuff below into separate class (also to be used by
    // the MetadataExportStrategy)

    try {
      fileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException("Error writing fileIndex.xml", e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(siarddkExportModule.getMainContainer(), path,
        siarddkExportModule.getWriteStrategy());

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_FILEINDEX,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        fileIndexFileStrategy.generateXML(null));

      writer.close();
    } catch (IOException e) {
      throw new ModuleException("Error writing fileIndex to the archive.", e);
    }

  }
}
