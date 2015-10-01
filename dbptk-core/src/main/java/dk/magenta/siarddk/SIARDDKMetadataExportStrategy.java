package dk.magenta.siarddk;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.common.SIARDMarshaller;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

  private static final String FILE_SEPERATOR = File.separator;

  private SIARDMarshaller siardMarshaller;
  private MetadataPathStrategy metadataPathStrategy;
  private FileIndexFileStrategy fileIndexFileStrategy;
  private Map<String, String> exportModuleArgs;

  public SIARDDKMetadataExportStrategy(SIARDDKExportModule siarddkExportModule) {
    siardMarshaller = siarddkExportModule.getSiardMarshaller();
    fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    exportModuleArgs = siarddkExportModule.getExportModuleArgs();
  }

  @Override
  public void writeMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // TO-DO: Refactor this into one method

    // Generate tableIndex.xml

    try {
      IndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy();
      String path = metadataPathStrategy.getXmlFilePath(Constants.TABLE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
      siardMarshaller.marshal("dk.magenta.siarddk.tableindex", "/schema/tableIndex.xsd",
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd", writer,
        tableIndexFileStrategy.generateXML(dbStructure));
      writer.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("Error writing tableIndex.xml to the archive.", e);
    }

    // Generate archiveIndex.xml

    if (exportModuleArgs.get(Constants.ARCHIVE_INDEX) != null) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(Constants.ARCHIVE_INDEX);
        OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy archiveIndexFileStrategy = new CommandLineIndexFileStrategy(Constants.ARCHIVE_INDEX,
          exportModuleArgs, writer);
        archiveIndexFileStrategy.generateXML(null);
        writer.close();

        fileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException("Error writing archiveIndex.xml to the archive");
      }
    }

    // Generate contextDocumentationIndex.xml

    if (exportModuleArgs.get(Constants.CONTEXT_DOCUMENTATION_INDEX) != null) {
      try {

        String path = metadataPathStrategy.getXmlFilePath(Constants.CONTEXT_DOCUMENTATION_INDEX);
        OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy contextDocumentationIndexFileStrategy = new CommandLineIndexFileStrategy(
          Constants.CONTEXT_DOCUMENTATION_INDEX, exportModuleArgs, writer);
        contextDocumentationIndexFileStrategy.generateXML(null);
        writer.close();

        fileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException("Error writing contextDocumentationIndex.xml to the archive");
      }
    }
  }

  @Override
  public void writeMetadataXSD(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // Write contents to Schemas/standard
    writeSchemaFile(outputContainer, Constants.XML_SCHEMA, writeStrategy);
    writeSchemaFile(outputContainer, Constants.TABLE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, Constants.ARCHIVE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, Constants.CONTEXT_DOCUMENTATION_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, Constants.FILE_INDEX, writeStrategy);

  }

  private void writeSchemaFile(SIARDArchiveContainer container, String indexFile, WriteStrategy writeStrategy)
    throws ModuleException {

    String filename = indexFile + ".xsd";

    InputStream inputStream = this.getClass().getResourceAsStream("/schema/" + filename);
    String path = "Schemas" + FILE_SEPERATOR + "standard" + FILE_SEPERATOR + filename;
    OutputStream outputStream = fileIndexFileStrategy.getWriter(container, path, writeStrategy);

    try {
      IOUtils.copy(inputStream, outputStream);
      inputStream.close();
      outputStream.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("There was an error writing " + filename, e);
    }
  }
}
