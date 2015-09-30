package com.databasepreservation.modules.siard.out.content;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ContentExportStrategy implements ContentExportStrategy {
  private final static String ENCODING = "UTF-8";
  private final Logger logger = Logger.getLogger(SIARD1ContentExportStrategy.class);
  private final ContentPathExportStrategy contentPathStrategy;
  private final WriteStrategy writeStrategy;
  private final SIARDArchiveContainer baseContainer;
  private final boolean prettyXMLOutput;
  XMLBufferedWriter currentWriter;
  OutputStream currentStream;
  SchemaStructure currentSchema;
  TableStructure currentTable;
  int currentRowIndex;
  private List<LargeObject> LOBsToExport;

  public SIARD1ContentExportStrategy(ContentPathExportStrategy contentPathStrategy, WriteStrategy writeStrategy,
    SIARDArchiveContainer baseContainer, boolean prettyXMLOutput) {
    this.contentPathStrategy = contentPathStrategy;
    this.writeStrategy = writeStrategy;
    this.baseContainer = baseContainer;

    this.LOBsToExport = new ArrayList<LargeObject>();
    currentRowIndex = -1;
    this.prettyXMLOutput = prettyXMLOutput;
  }

  @Override
  public void openSchema(SchemaStructure schema) throws ModuleException {
    currentSchema = schema;
  }

  @Override
  public void closeSchema(SchemaStructure schema) throws ModuleException {
    // do nothing
  }

  @Override
  public void openTable(TableStructure table) throws ModuleException {
    currentStream = writeStrategy.createOutputStream(baseContainer,
      contentPathStrategy.getTableXmlFilePath(currentSchema.getIndex(), table.getIndex()));
    currentWriter = new XMLBufferedWriter(currentStream, prettyXMLOutput);
    currentTable = table;
    currentRowIndex = 0;
    LOBsToExport = new ArrayList<LargeObject>();

    try {
      writeXmlOpenTable();
    } catch (IOException e) {
      throw new ModuleException("Error handling open table " + table.getId(), e);
    }
  }

  @Override
  public void closeTable(TableStructure table) throws ModuleException {
    // finish writing the table
    try {
      writeXmlCloseTable();
      currentWriter.close();
    } catch (IOException e) {
      throw new ModuleException("Error handling close table " + table.getId(), e);
    }

    // write lobs if they have not been written yet
    try {
      if (!writeStrategy.isSimultaneousWritingSupported()) {
        for (LargeObject largeObject : LOBsToExport) {
          writeLOB(largeObject);
        }
      }
    } catch (IOException e) {
      throw new ModuleException("Error writing LOBs", e);
    }

    // export table XSD
    try {
      writeXsd();
    } catch (IOException e) {
      throw new ModuleException("Error writing table XSD", e);
    }

    // reset variables
    currentTable = null;
    currentRowIndex = 0;
  }

  @Override
  public void tableRow(Row row) throws ModuleException {
    try {
      currentWriter.openTag("row", 1);

      // note about columnIndex: array columnIndex starts at 0 but column
      // columnIndex starts at 1,
      // that is why it is incremented halfway through the loop and not at the
      // beginning nor at the end
      int columnIndex = 0;
      for (Cell cell : row.getCells()) {
        ColumnStructure column = currentTable.getColumns().get(columnIndex);
        columnIndex++;
        if (cell instanceof BinaryCell) {
          writeBinaryCell(cell, column, columnIndex);
        } else if (cell instanceof SimpleCell) {
          writeSimpleCell(cell, column, columnIndex);
        } else if (cell instanceof ComposedCell) {
          writeComposedCell(cell, column, columnIndex);
        }
        // TODO add support for composed cell types
      }

      currentWriter.closeTag("row", 1);
      currentRowIndex++;
    } catch (IOException e) {
      throw new ModuleException("Could not write row" + row.toString(), e);
    }
  }

  private void writeComposedCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException,
    IOException {
    logger.error("Composed cell writing is not yet implemented");
  }

  private void writeSimpleCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    SimpleCell simpleCell = (SimpleCell) cell;

    // deal with strings that are big enough to be saved externally as if they
    // were CLOBs
    if (column.getType() instanceof SimpleTypeString && simpleCell.getSimpledata() != null
      && simpleCell.getSimpledata().length() > 4000) {// TODO: used value from
                                                      // original code, but why
                                                      // 4000?
      writeLargeObjectData(cell, columnIndex);
    } else {
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  private void writeBinaryCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    BinaryCell binaryCell = (BinaryCell) cell;

    long length = binaryCell.getLength();
    if (length > 2000) {// TODO: used value from original code, but why 2000?
      writeLargeObjectData(cell, columnIndex);
    } else {
      SimpleCell simpleCell = new SimpleCell(binaryCell.getId());
      if (length == 0) {
        simpleCell.setSimpledata(null);
      } else {
        InputStream inputStream = binaryCell.getInputstream();
        byte[] bytes = IOUtils.toByteArray(inputStream);
        inputStream.close();
        simpleCell.setSimpledata(Hex.encodeHexString(bytes));
      }
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  private void writeSimpleCellData(SimpleCell simpleCell, int columnIndex) throws IOException {
    currentWriter.inlineOpenTag("c" + columnIndex, 2);

    if (simpleCell.getSimpledata() != null) {
      currentWriter.write(XMLUtils.encode(simpleCell.getSimpledata()));
    }

    currentWriter.closeTag("c" + columnIndex);
  }

  private void writeLargeObjectData(Cell cell, int columnIndex) throws IOException, ModuleException {
    currentWriter.beginOpenTag("c" + columnIndex, 2).space().append("file=\"");

    LargeObject lob = null;

    if (cell instanceof BinaryCell) {
      BinaryCell binCell = (BinaryCell) cell;

      String path = contentPathStrategy.getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
        currentRowIndex + 1);

      // blob header
      currentWriter.append(path).append('"').space().append("length=\"").append(String.valueOf(binCell.getLength()))
        .append("\"");

      try {
        lob = new LargeObject(binCell.getInputstream(), path);
      } catch (ModuleException e) {
        throw new ModuleException("Error getting blob data");
      }
    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;

      String path = contentPathStrategy.getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
        currentRowIndex + 1);

      // blob header
      currentWriter.append(path).append('"').space().append("length=\"")
        .append(String.valueOf(txtCell.getSimpledata().length())).append("\"");

      // workaround to have data from CLOBs saved as a temporary file to be read
      String data = txtCell.getSimpledata();
      try {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes());
        FileItem fileItem = new FileItem(inputStream);
        inputStream.close();
        lob = new LargeObject(fileItem.getInputStream(), path);
      } catch (ModuleException e) {
        throw new ModuleException("Error getting clob data");
      }
    }

    // decide to whether write the LOB right away or later
    if (writeStrategy.isSimultaneousWritingSupported()) {
      writeLOB(lob);
    } else {
      LOBsToExport.add(lob);
    }

    currentWriter.endShorthandTag();
  }

  private void writeXmlOpenTable() throws IOException {
    currentWriter
      .append("<?xml version=\"1.0\" encoding=\"")
      .append(ENCODING)
      .append("\"?>")
      .newline()

      .beginOpenTag("table", 0)

      .appendAttribute(
        "xsi:schemaLocation",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(),
          currentTable.getIndex()) + " " + contentPathStrategy.getTableXsdFileName(currentTable.getIndex()))

      .appendAttribute(
        "xmlns",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(),
          currentTable.getIndex()))

      .appendAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

      .endOpenTag();
  }

  private void writeXmlCloseTable() throws IOException {
    currentWriter.closeTag("table", 0);
  }

  private void writeLOB(LargeObject lob) throws ModuleException, IOException {
    OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getPath());
    InputStream in = lob.getDatasource();

    // copy lob to output
    IOUtils.copy(in, out);

    // close resources
    in.close();
    out.close();
  }

  private void writeXsd() throws IOException, ModuleException {
    OutputStream xsdStream = writeStrategy.createOutputStream(baseContainer,
      contentPathStrategy.getTableXsdFilePath(currentSchema.getIndex(), currentTable.getIndex()));
    XMLBufferedWriter xsdWriter = new XMLBufferedWriter(xsdStream, prettyXMLOutput);

    xsdWriter
      // ?xml tag
      .append("<?xml version=\"1.0\" encoding=\"")
      .append(ENCODING)
      .append("\"?>")
      .newline()

      // xs:schema tag
      .beginOpenTag("xs:schema", 0)
      .appendAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema")

      .appendAttribute(
        "xmlns",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(),
          currentTable.getIndex()))

      .appendAttribute("attributeFormDefault", "unqualified")

      .appendAttribute("elementFormDefault", "qualified")

      .appendAttribute(
        "targetNamespace",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(),
          currentTable.getIndex()))

      .endOpenTag()

      // xs:element name="table"
      .beginOpenTag("xs:element", 1).appendAttribute("name", "table").endOpenTag()

      .openTag("xs:complexType", 2)

      .openTag("xs:sequence", 3)

      .beginOpenTag("xs:element", 4).appendAttribute("maxOccurs", "unbounded").appendAttribute("minOccurs", "0")
      .appendAttribute("name", "row").appendAttribute("type", "rowType").endShorthandTag()

      .closeTag("xs:sequence", 3)

      .closeTag("xs:complexType", 2)

      .closeTag("xs:element", 1)

      // xs:complexType name="rowType"

      .beginOpenTag("xs:complexType", 1).appendAttribute("name", "rowType").endOpenTag()

      .openTag("xs:sequence", 2);

    // insert all <xs:element> in <xs:complexType name="rowType">
    int columnIndex = 1;
    for (ColumnStructure col : currentTable.getColumns()) {
      try {
        String xsdType = sql99toXSDType.convert(col.getType());

        xsdWriter.beginOpenTag("xs:element", 4);

        if (col.isNillable()) {
          xsdWriter.appendAttribute("minOccurs", "0");
        }

        xsdWriter.appendAttribute("name", "c" + columnIndex).appendAttribute("type", xsdType).endShorthandTag();
      } catch (ModuleException e) {
        logger.error(String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
      } catch (UnknownTypeException e) {
        logger.error(String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
      }
      columnIndex++;
    }

    xsdWriter
      // close tags for xs:sequence and xs:complexType
      .closeTag("xs:sequence", 2)

      .closeTag("xs:complexType", 1)

      // xs:complexType name="clobType"
      .beginOpenTag("xs:complexType", 1).appendAttribute("name", "clobType").endOpenTag()

      .openTag("xs:simpleContent", 2)

      .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:string").endOpenTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1)

      // xs:complexType name="blobType"
      .beginOpenTag("xs:complexType", 1).appendAttribute("name", "blobType").endOpenTag()

      .openTag("xs:simpleContent", 2)

      .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:string").endOpenTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1)

      // close schema
      .closeTag("xs:schema");

    xsdWriter.close();
  }

}
