package dk.magenta.siarddk;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.output.SIARDExportDefault;

import dk.magenta.common.SIARDMarshaller;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKDatabaseExportModule extends SIARDExportDefault {

  private SIARDDKExportModule siarddkExportModule;
  private final Logger logger = Logger.getLogger(SIARDDKDatabaseExportModule.class);

  public SIARDDKDatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    super(siarddkExportModule.getContentExportStrategy(), siarddkExportModule.getMainContainer(), siarddkExportModule
      .getWriteStrategy(), siarddkExportModule.getMetadataExportStrategy());

    this.siarddkExportModule = siarddkExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    // Delete output folder if it already exists

    File outputFolder = siarddkExportModule.getMainContainer().getPath().toFile();
    if (outputFolder.isDirectory()) {
      try {
        FileUtils.deleteDirectory(outputFolder);

        // TO-DO: not logging ?

        logger.info("Deleted the already existing folder: " + outputFolder);
      } catch (IOException e) {
        e.printStackTrace();
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

    if (exportModuleArgs.get(Constants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      ContextDocumentationWriter contextDocumentationWriter = new ContextDocumentationWriter(
        siarddkExportModule.getMainContainer(), siarddkExportModule.getWriteStrategy(), fileIndexFileStrategy,
        siarddkExportModule.getExportModuleArgs());

      contextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    try {
      fileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException("Error writing fileIndex.xml", e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(Constants.FILE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(siarddkExportModule.getMainContainer(), path,
        siarddkExportModule.getWriteStrategy());
      siardMarshaller.marshal("dk.magenta.siarddk.fileindex", "/siarddk/fileIndex.xsd",
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        fileIndexFileStrategy.generateXML(null));
      writer.close();
    } catch (IOException e) {
      throw new ModuleException("Error writing fileIndex to the archive.", e);
    }

  }
}
