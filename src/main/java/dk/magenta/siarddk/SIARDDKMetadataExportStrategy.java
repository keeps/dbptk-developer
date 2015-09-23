package dk.magenta.siarddk;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.common.SIARDMarshaller;

public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

  private static final String FILE_SEPERATOR = File.separator;
  private static final String CONTEXT_DOCUMENTATION_INDEX = "contextDocumentationIndex";
  private static final String ARCHIVE_INDEX = "archiveIndex";
  private static final String TABLE_INDEX = "tableIndex";
  private static final String FILE_INDEX = "fileIndex";
  private static final String XML_SCHEMA = "XMLSchema";

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
      String path = metadataPathStrategy.getXmlFilePath(TABLE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
      siardMarshaller.marshal("dk.magenta.siarddk.tableindex", "/siarddk/tableIndex.xsd",
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd", writer,
        tableIndexFileStrategy.generateXML(dbStructure));
      writer.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("Error writing tableIndex.xml to the archive.", e);
    }

    // Generate archiveIndex.xml

    try {
      String path = metadataPathStrategy.getXmlFilePath(ARCHIVE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
      IndexFileStrategy archiveIndexFileStrategy = new CommandLineIndexFileStrategy(ARCHIVE_INDEX, exportModuleArgs,
        writer);
      archiveIndexFileStrategy.generateXML(null);
      writer.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("Error writing archiveIndex.xml to the archive");
    }

    // Generate contextDocumentationIndex.xml

    try {

      String path = metadataPathStrategy.getXmlFilePath(CONTEXT_DOCUMENTATION_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
      IndexFileStrategy contextDocumentationIndexFileStrategy = new CommandLineIndexFileStrategy(
        CONTEXT_DOCUMENTATION_INDEX, exportModuleArgs, writer);
      contextDocumentationIndexFileStrategy.generateXML(null);
      writer.close();

      fileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException("Error writing contextDocumentationIndex.xml to the archive");
    }

  }

  @Override
  public void writeMetadataXSD(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {

    // Write contents to Schemas/standard
    writeSchemaFile(outputContainer, XML_SCHEMA, writeStrategy);
    writeSchemaFile(outputContainer, TABLE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, ARCHIVE_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, CONTEXT_DOCUMENTATION_INDEX, writeStrategy);
    writeSchemaFile(outputContainer, FILE_INDEX, writeStrategy);

    // Generate fileIndex.xml

    try {
      fileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException("Error writing fileIndex.xml", e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(FILE_INDEX);
      OutputStream writer = fileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
      siardMarshaller.marshal("dk.magenta.siarddk.fileindex", "/siarddk/fileIndex.xsd",
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        fileIndexFileStrategy.generateXML(null));
      writer.close();
    } catch (IOException e) {
      throw new ModuleException("Error writing fileIndex to the archive.", e);
    }

  }

  private void writeSchemaFile(SIARDArchiveContainer container, String indexFile, WriteStrategy writeStrategy)
    throws ModuleException {

    String filename = indexFile + ".xsd";

    InputStream inputStream = this.getClass().getResourceAsStream("/siarddk/" + filename);
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

  private DatabaseStructure generateDatabaseStructure() {

    // For testing marshaller

    // ////////////////// Create database structure //////////////////////

    ColumnStructure columnStructure = new ColumnStructure();
    columnStructure.setName("c1");
    Type type = new SimpleTypeString(20, true);
    type.setSql99TypeName("boolean"); // Giving a non-sql99 type will make
    // marshaller fail
    columnStructure.setType(type);
    List<ColumnStructure> columnList = new ArrayList<ColumnStructure>();
    columnList.add(columnStructure);
    TableStructure tableStructure = new TableStructure();
    tableStructure.setName("table1");
    tableStructure.setColumns(columnList);
    List<TableStructure> tableList = new ArrayList<TableStructure>();
    tableList.add(tableStructure);
    SchemaStructure schemaStructure = new SchemaStructure();
    schemaStructure.setTables(tableList);
    List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
    schemaList.add(schemaStructure);
    DatabaseStructure dbStructure = new DatabaseStructure();
    dbStructure.setName("test");
    dbStructure.setSchemas(schemaList);

    return dbStructure;

    // ///////////////////////////////////////////////////////////////////

  }

}
