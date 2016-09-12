package com.databasepreservation.modules.siard.out.metadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

  private SIARDMarshaller siardMarshaller;
  private MetadataPathStrategy metadataPathStrategy;
  private FileIndexFileStrategy fileIndexFileStrategy;
  private DocIndexFileStrategy docIndexFileStrategy;
  private Map<String, String> exportModuleArgs;
  private LOBsTracker lobsTracker;

  public SIARDDKMetadataExportStrategy(SIARDDKExportModule siarddkExportModule) {
    siardMarshaller = siarddkExportModule.getSiardMarshaller();
    fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    docIndexFileStrategy = siarddkExportModule.getDocIndexFileStrategy();
    metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    lobsTracker = siarddkExportModule.getLobsTracker();
  }

  @Override
  public void writeMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // TO-DO: Refactor this into one method in class that can be used by
    // SIARDDKDatabaseExportModule also

    // Generate tableIndex.xml

    try {
      IndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(lobsTracker);
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_TABLEINDEX,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.TABLE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd", writer,
        tableIndexFileStrategy.generateXML(dbStructure));

      writer.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("Error writing tableIndex.xml to the archive.", e);
    }

    // Generate archiveIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.ARCHIVE_INDEX) != null) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.ARCHIVE_INDEX);
        OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy archiveIndexFileStrategy = new CommandLineIndexFileStrategy(SIARDDKConstants.ARCHIVE_INDEX,
          exportModuleArgs, writer, metadataPathStrategy);
        archiveIndexFileStrategy.generateXML(null);
        writer.close();

        fileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException("Error writing archiveIndex.xml to the archive", e);
      }
    }

    // Generate contextDocumentationIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX) != null) {
      try {

        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX);
        OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy contextDocumentationIndexFileStrategy = new CommandLineIndexFileStrategy(
          SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX, exportModuleArgs, writer, metadataPathStrategy);
        contextDocumentationIndexFileStrategy.generateXML(null);
        writer.close();

        fileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException("Error writing contextDocumentationIndex.xml to the archive", e);
      }
    }

    if (lobsTracker.getLOBsCount() > 0) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX);
        OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

        siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_DOCINDEX,
          metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.DOC_INDEX),
          "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/docIndex.xsd", writer,
          docIndexFileStrategy.generateXML(dbStructure));

        writer.close();

        fileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException("Error writing docIndex.xml to the archive.", e);
      }
    }

    createLocalSharedFolder(outputContainer);

  }

  @Override
  public void writeMetadataXSD(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // Write contents to Schemas/standard
    writeSchemaFile(outputContainer, SIARDDKConstants.XML_SCHEMA, writeStrategy);
    writeSchemaFile(outputContainer, SIARDDKConstants.TABLE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, SIARDDKConstants.ARCHIVE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, SIARDDKConstants.FILE_INDEX, writeStrategy);
    if (lobsTracker.getLOBsCount() > 0) {
      writeSchemaFile(outputContainer, SIARDDKConstants.DOC_INDEX, writeStrategy);
    }
  }

  private void writeSchemaFile(SIARDArchiveContainer container, String indexFile, WriteStrategy writeStrategy)
    throws ModuleException {

    InputStream inputStream = this.getClass().getResourceAsStream(metadataPathStrategy.getXsdResourcePath(indexFile));

    String path = metadataPathStrategy.getXsdFilePath(indexFile);

    OutputStream outputStream = fileIndexFileStrategy.getWriter(container, path, writeStrategy);

    try {
      IOUtils.copy(inputStream, outputStream);
      inputStream.close();
      outputStream.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("There was an error writing " + indexFile + ".xsd", e);
    }
  }

  private void createLocalSharedFolder(SIARDArchiveContainer container) {

    Path containerPath = container.getPath();
    Path localShared = Paths.get("Schemas/localShared");
    File folder = containerPath.resolve(localShared).toFile();
    System.out.println(folder);
    try {
      System.out.println(folder.mkdirs());
    } catch (SecurityException e) {
      e.printStackTrace();
    }
  }
}
