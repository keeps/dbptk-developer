/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.adapters.SIARDDKAdapter;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDKMetadataExportStrategy.class);

  private SIARDMarshaller siardMarshaller;
  private MetadataPathStrategy metadataPathStrategy;
  private SIARDDKFileIndexFileStrategy SIARDDKFileIndexFileStrategy;
  private SIARDDKDocIndexFileStrategy SIARDDKDocIndexFileStrategy;
  private Map<String, String> exportModuleArgs;
  private LOBsTracker lobsTracker;
  private SIARDDKAdapter siarddkAdapter;

  private Reporter reporter;

  public SIARDDKMetadataExportStrategy(SIARDDKExportModule siarddkExportModule, SIARDDKAdapter siarddkAdapter) {
    siardMarshaller = siarddkExportModule.getSiardMarshaller();
    SIARDDKFileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    SIARDDKDocIndexFileStrategy = siarddkExportModule.getDocIndexFileStrategy();
    metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    lobsTracker = siarddkExportModule.getLobsTracker();
    this.siarddkAdapter = siarddkAdapter;
  }

  @Override
  public void writeMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // TO-DO: Refactor this into one method in class that can be used by
    // SIARDDKDatabaseExportModule also

    // Generate tableIndex.xml

    try {
      IndexFileStrategy tableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(lobsTracker, siarddkAdapter);
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX);
      OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

      siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_TABLEINDEX,
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.TABLE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd", writer,
        tableIndexFileStrategy.generateXML(dbStructure));

      writer.close();

      SIARDDKFileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing tableIndex.xml to the archive.").withCause(e);
    }

    // Generate archiveIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.ARCHIVE_INDEX) != null) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.ARCHIVE_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy archiveIndexFileStrategy = new CommandLineIndexFileStrategy(SIARDDKConstants.ARCHIVE_INDEX,
          exportModuleArgs, writer, metadataPathStrategy);
        archiveIndexFileStrategy.generateXML(null);
        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing archiveIndex.xml to the archive").withCause(e);
      }
    }

    // Generate contextDocumentationIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX) != null) {
      try {

        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy contextDocumentationIndexFileStrategy = new CommandLineIndexFileStrategy(
          SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX, exportModuleArgs, writer, metadataPathStrategy);
        contextDocumentationIndexFileStrategy.generateXML(null);
        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing contextDocumentationIndex.xml to the archive")
          .withCause(e);
      }
    }

    if (lobsTracker.getLOBsCount() > 0) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

        siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_DOCINDEX,
          metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.DOC_INDEX),
          "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/docIndex.xsd", writer,
          SIARDDKDocIndexFileStrategy.generateXML(dbStructure));

        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing docIndex.xml to the archive.").withCause(e);
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

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  private void writeSchemaFile(SIARDArchiveContainer container, String indexFile, WriteStrategy writeStrategy)
    throws ModuleException {

    InputStream inputStream = this.getClass().getResourceAsStream(metadataPathStrategy.getXsdResourcePath(indexFile));

    String path = metadataPathStrategy.getXsdFilePath(indexFile);
    if (indexFile.contains("original")) {
      Path fullPath = Paths.get(path);
      Path pathToFolder = Paths.get(path).getParent();
      Path pathToFile = fullPath.getFileName();

      String fileName = pathToFile.toString().split("_")[0] + ".xsd";
      fullPath = pathToFolder.resolve(fileName);
      path = fullPath.toString();
      // System.out.println(path);
    }

    OutputStream outputStream = SIARDDKFileIndexFileStrategy.getWriter(container, path, writeStrategy);

    try {
      if (inputStream != null) {
        IOUtils.copy(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
      }

      SIARDDKFileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException().withMessage("There was an error writing " + indexFile + ".xsd").withCause(e);
    }
  }

  private void createLocalSharedFolder(SIARDArchiveContainer container) {
    Path containerPath = container.getPath();
    Path localShared = Paths.get("Schemas/localShared");
    File folder = containerPath.resolve(localShared).toFile();
    try {
      folder.mkdirs();
    } catch (SecurityException e) {
      LOGGER.error("Could not create directories", e);
    }
  }

}
