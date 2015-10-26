package com.databasepreservation.modules.siard.out.content;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.tika.Tika;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.metadata.FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKContentExportStrategy implements ContentExportStrategy {

  private final static String ENCODING = "utf-8";
  private final static String TAB = "  ";
  private final static String namespaceBase = "http://www.sa.dk/xmlns/siard/1.0/";

  private int tableCounter;

  private static final Logger logger = Logger.getLogger(SIARDDKContentExportStrategy.class);

  private ContentPathExportStrategy contentPathExportStrategy;
  private FileIndexFileStrategy fileIndexFileStrategy;
  private SIARDArchiveContainer baseContainer;
  private OutputStream currentStream;
  private BufferedWriter currentWriter;
  private WriteStrategy writeStrategy;
  private LOBsTracker lobsTracker;
  private MimetypeHandler mimetypeHandler;

  public SIARDDKContentExportStrategy(SIARDDKExportModule siarddkExportModule) {

    tableCounter = 1;

    mimetypeHandler = new SIARDDKMimetypeHandler();

    contentPathExportStrategy = siarddkExportModule.getContentPathExportStrategy();
    fileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    baseContainer = siarddkExportModule.getMainContainer();
    writeStrategy = siarddkExportModule.getWriteStrategy();
    lobsTracker = siarddkExportModule.getLobsTracker();
  }

  @Override
  public void openSchema(SchemaStructure schema) throws ModuleException {
    // Nothing to do

  }

  @Override
  public void closeSchema(SchemaStructure schema) throws ModuleException {
    // Nothing to do

  }

  @Override
  public void openTable(TableStructure tableStructure) throws ModuleException {

    currentStream = fileIndexFileStrategy.getWriter(baseContainer,
      contentPathExportStrategy.getTableXmlFilePath(0, tableStructure.getIndex()), writeStrategy);
    currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));

    // Note: cannot use JAXB or JDOM to generate XML for tables, since the
    // actual tables are too large

    StringBuilder builder = new StringBuilder();
    builder
      .append("<?xml version=\"1.0\" encoding=\"")
      .append(ENCODING)
      .append("\"?>\n")

      .append("<table xsi:schemaLocation=\"")
      .append(
        contentPathExportStrategy.getTableXsdNamespace(namespaceBase, tableStructure.getIndex(),
          tableStructure.getIndex()))
      .append(" ")
      .append(contentPathExportStrategy.getTableXsdFileName(tableStructure.getIndex()))
      .append("\" ")
      .append("xmlns=\"")
      .append(
        contentPathExportStrategy.getTableXsdNamespace(namespaceBase, tableStructure.getIndex(),
          tableStructure.getIndex())).append("\" ").append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"")
      .append(">").append("\n");

    try {
      currentWriter.write(builder.toString());
    } catch (IOException e) {
      throw new ModuleException("Error handling open table " + tableStructure.getId(), e);
    }

    // List for the LOBs tracker
    // LOBsColumns = new ArrayList<Integer>();
  }

  @Override
  public void closeTable(TableStructure tableStructure) throws ModuleException {
    try {
      currentWriter.write("</table>");
      currentWriter.close();

      fileIndexFileStrategy.addFile(contentPathExportStrategy.getTableXmlFilePath(0, tableStructure.getIndex()));

      tableCounter += 1;
      // lobsTracker.incrementTableCount();

    } catch (IOException e) {
      throw new ModuleException("Error handling close table " + tableStructure.getId(), e);
    }

    // Code to write table XSDs

    // Set namespaces for schema
    Namespace defaultNamespace = Namespace.getNamespace(contentPathExportStrategy.getTableXsdNamespace(namespaceBase,
      tableStructure.getIndex(), tableStructure.getIndex()));
    Namespace xs = Namespace.getNamespace("xs", "http://www.w3.org/2001/XMLSchema");

    // Create root element
    Element schema = new Element("schema", xs);
    schema.addNamespaceDeclaration(defaultNamespace);
    schema.setAttribute(
      "targetNamespace",
      contentPathExportStrategy.getTableXsdNamespace(namespaceBase, tableStructure.getIndex(),
        tableStructure.getIndex()));
    schema.setAttribute("elementFormDefault", "qualified");
    schema.setAttribute("attributeFormDefault", "unqualified");

    // Create table element
    Element table = new Element("element", xs);
    table.setAttribute("name", "table");

    // Create complex type for table
    Element complexTypeTable = new Element("complexType", xs);
    Element sequenceTable = new Element("sequence", xs);
    Element element = new Element("element", xs);
    element.setAttribute("minOccurs", "0");
    element.setAttribute("maxOccurs", "unbounded");
    element.setAttribute("name", "row");
    element.setAttribute("type", "rowType");

    // Add elements to appropriate ancestors
    sequenceTable.addContent(element);
    complexTypeTable.addContent(sequenceTable);
    table.addContent(complexTypeTable);
    schema.addContent(table);

    // Create complex type for rowType
    Element complexTypeRowType = new Element("complexType", xs);
    complexTypeRowType.setAttribute("name", "rowType");
    Element sequenceRowType = new Element("sequence", xs);

    // Create elements containing column info
    int columnIndex = 1;
    List<ColumnStructure> columns = tableStructure.getColumns();
    for (ColumnStructure columnStructure : columns) {
      Element c = new Element("element", xs);
      c.setAttribute("name", "c" + Integer.toString(columnIndex));
      String sql99Type = columnStructure.getType().getSql99TypeName();
      c.setAttribute("type", SIARDDKsql99ToXsdType.convert(sql99Type));
      if (columnStructure.getNillable()) {
        c.setAttribute("nillable", "true");
      }
      sequenceRowType.addContent(c);
      columnIndex++;
    }

    // Add elements to appropriate ancestors
    complexTypeRowType.addContent(sequenceRowType);
    schema.addContent(complexTypeRowType);

    // Write schema to archive
    currentStream = fileIndexFileStrategy.getWriter(baseContainer,
      contentPathExportStrategy.getTableXsdFilePath(0, tableStructure.getIndex()), writeStrategy);
    currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));

    Document d = new Document(schema);
    XMLOutputter outputter = new XMLOutputter();
    try {
      // outputter.output(d, System.out);
      outputter.output(d, currentWriter);
      currentWriter.close();

      fileIndexFileStrategy.addFile(contentPathExportStrategy.getTableXsdFilePath(0, tableStructure.getIndex()));

    } catch (IOException e) {
      throw new ModuleException("Could not write table" + tableStructure.getIndex() + " to disk", e);
    }
  }

  @Override
  public void tableRow(Row row) throws ModuleException {
    try {

      currentWriter.append(TAB).append("<row>\n");

      int columnIndex = 0;
      for (Cell cell : row.getCells()) {
        columnIndex++;
        if (cell instanceof SimpleCell) {

          SimpleCell simpleCell = (SimpleCell) cell;
          if (simpleCell.getSimpledata() != null) {
            currentWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
              .append(XMLUtils.encode(simpleCell.getSimpledata())).append("</c").append(String.valueOf(columnIndex))
              .append(">\n");
          } else {
            currentWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex))
              .append(" xsi:nil=\"true\"/>").append("\n");
          }

        } else if (cell instanceof BinaryCell) {

          // TO-DO: should it be checked if cell content is null?
          // TO-DO: should LOBcounter be encoded? (probably not)

          currentWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
            .append(Integer.toString(lobsTracker.getLOBsCount())).append("</c").append(String.valueOf(columnIndex))
            .append(">\n");

          lobsTracker.addLOB(tableCounter, columnIndex);
          BinaryCell binaryCell = (BinaryCell) cell;

          // Determine the mimetype (Tika should use an inputstream which
          // supports marks)
          InputStream is = new BufferedInputStream(binaryCell.getInputstream());
          Tika tika = new Tika();
          String mimeType = tika.detect(is); // Resets the inputstream after use
          System.out.println(mimeType);

          // In SIARDDK the only accepted mimetypes are image/tiff and JPEG2000
          if (mimetypeHandler.isMimetypeAllowed(mimeType)) {
            // Archive BLOB - simultaneous writing always supported for SIARDDK
            // Create LargeObject (lob)
            // Lav BLOB path strategy

            String path = contentPathExportStrategy.getBlobFilePath(-1, -1, -1, -1)
              + mimetypeHandler.getFileExtension(mimeType);
            System.out.println(path);

            LargeObject blob = null;

            try {
              blob = new LargeObject(binaryCell.getInputstream(), path);
            } catch (ModuleException e) {
              throw new ModuleException("Error getting blob data");
            }

            // Write the BLOB
            OutputStream out = writeStrategy.createOutputStream(baseContainer, blob.getPath());
            InputStream in = blob.getDatasource();
            IOUtils.copy(in, out);
            in.close();
            out.close();

          } else {
            System.out.println("Detected mimetype: " + mimeType);
            logger.error("Unaccepted mimetype for BLOB!");
          }

        } else if (cell instanceof ComposedCell) {
          throw new ModuleException("Cannot handle composed cells yet");
        }
      }

      currentWriter.append(TAB).append("</row>\n");

    } catch (IOException e) {
      throw new ModuleException("Could not write row " + row.toString(), e);
    }
  }
}
