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
import com.databasepreservation.modules.siard.common.ProvidesInputStream;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentExportStrategy implements ContentExportStrategy {
  private final static String ENCODING = "UTF-8";
  private final Logger logger = Logger.getLogger(SIARD1ContentExportStrategy.class);
  private final SIARD2ContentPathExportStrategy contentPathStrategy;
  private final WriteStrategy writeStrategy;
  private final SIARDArchiveContainer baseContainer;
  private final boolean prettyXMLOutput;
  XMLBufferedWriter currentWriter;
  OutputStream currentStream;
  SchemaStructure currentSchema;
  TableStructure currentTable;
  int currentRowIndex;
  private List<LargeObject> LOBsToExport;

  public SIARD2ContentExportStrategy(SIARD2ContentPathExportStrategy contentPathStrategy, WriteStrategy writeStrategy,
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
        InputStream inputStream = binaryCell.createInputstream();
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
      final BinaryCell binCell = (BinaryCell) cell;

      // blob header
      currentWriter.append(contentPathStrategy.getBlobFileName(currentRowIndex + 1)).append('"').space()
        .append("length=\"").append(String.valueOf(binCell.getLength()))
        .append("\"");

      lob = new LargeObject(new ProvidesInputStream() {
        @Override
        public InputStream createInputStream() throws ModuleException {
          return binCell.createInputstream();
        }
      }, contentPathStrategy.getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
        currentRowIndex + 1));

    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;

      // clob header
      currentWriter.append(contentPathStrategy.getClobFileName(currentRowIndex + 1)).append('"').space()
        .append("length=\"")
        .append(String.valueOf(txtCell.getSimpledata().length())).append("\"");

      // workaround to have data from CLOBs saved as a temporary file to be read
      String data = txtCell.getSimpledata();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes());
      try {
        final FileItem fileItem = new FileItem(inputStream);
        lob = new LargeObject(new ProvidesInputStream() {
          @Override public InputStream createInputStream() throws ModuleException {
            return fileItem.createInputStream();
          }
        }, contentPathStrategy
          .getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex, currentRowIndex + 1));
      } finally {
        inputStream.close();
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
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/2.0/", currentSchema.getIndex(),
          currentTable.getIndex()) + " " + contentPathStrategy.getTableXsdFileName(currentTable.getIndex()))

      .appendAttribute(
        "xmlns",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/2.0/", currentSchema.getIndex(),
          currentTable.getIndex()))

      .appendAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

      .endOpenTag();
  }

  private void writeXmlCloseTable() throws IOException {
    currentWriter.closeTag("table", 0);
  }

  private void writeLOB(LargeObject lob) throws ModuleException, IOException {
    OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getOutputPath());
    InputStream in = lob.getInputStreamProvider().createInputStream();

    logger.info("Writing lob to " + lob.getOutputPath());

    // copy lob to output
    try {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new ModuleException("Could not write lob", e);
    } finally {
      // close resources
      try {
        in.close();
        out.close();
      } catch (IOException e) {
        logger.warn("Could not cleanup lob resources", e);
      }
    }
  }

  private void writeXsd() throws IOException, ModuleException {
    OutputStream xsdStream = writeStrategy.createOutputStream(baseContainer,
      contentPathStrategy.getTableXsdFilePath(currentSchema.getIndex(), currentTable.getIndex()));
    XMLBufferedWriter xsdWriter = new XMLBufferedWriter(xsdStream, prettyXMLOutput);

    xsdWriter
      // ?xml tag
      .append("<?xml version=\"1.0\" encoding=\"")
      .append(ENCODING)
      .append("\" standalone=\"no\"?>")
      .newline()

      // xs:schema tag
      .beginOpenTag("xs:schema", 0)
      .appendAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema")

      .appendAttribute(
        "xmlns",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/2.0/", currentSchema.getIndex(),
          currentTable.getIndex()))

      .appendAttribute("attributeFormDefault", "unqualified")

      .appendAttribute("elementFormDefault", "qualified")

      .appendAttribute(
        "targetNamespace",
        contentPathStrategy.getTableXsdNamespace("http://www.admin.ch/xmlns/siard/2.0/", currentSchema.getIndex(),
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

      .closeTag("xs:complexType", 1);

    // xs:complexType name="clobType"
    xsdWriter.beginOpenTag("xs:complexType", 1).appendAttribute("name", "clobType").endOpenTag()

    .openTag("xs:simpleContent", 2)

    .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:string").endOpenTag()

    .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:anyURI")
      .appendAttribute("use", "required").endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .appendAttribute("use", "required").endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "messageDigest").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1);

    // xs:complexType name="blobType"
    xsdWriter.beginOpenTag("xs:complexType", 1).appendAttribute("name", "blobType").endOpenTag()

    .openTag("xs:simpleContent", 2)

    .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:hexBinary").endOpenTag()

    .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:anyURI")
      .appendAttribute("use", "required").endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .appendAttribute("use", "required").endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "messageDigest").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1);

    // xs:simpleType name="dateType"
    xsdWriter.beginOpenTag("xs:simpleType", 1).appendAttribute("name", "dateType").endOpenTag()

    .openTag("xs:annotation", 2)

    .openTag("xs:documentation", 3)
      .append("dateType restricts xs:date to dates between 0001 and 9999 and is in UTC (no +/- but an optional Z)")
      .closeTag("xs:documentation", 3)

      .closeTag("xs:annotation", 2)

      .beginOpenTag("xs:restriction", 2).appendAttribute("base", "xs:date").endOpenTag()

      .beginOpenTag("xs:minInclusive", 3).appendAttribute("value", "0001-01-01Z").endShorthandTag()

      .beginOpenTag("xs:maxExclusive", 3).appendAttribute("value", "10000-01-01Z").endShorthandTag()

      .beginOpenTag("xs:pattern", 3).appendAttribute("value", "\\d{4}-\\d{2}-\\d{2}Z?").endShorthandTag()

      .closeTag("xs:restriction", 2)

      .closeTag("xs:simpleType", 1);

    // xs:simpleType name="timeType"
    xsdWriter.beginOpenTag("xs:simpleType", 1).appendAttribute("name", "timeType").endOpenTag()

    .openTag("xs:annotation", 2)

    .openTag("xs:documentation", 3)
      .append("timeType restricts xs:date to dates between 0001 and 9999 and is in UTC (no +/- but an optional Z)")
      .closeTag("xs:documentation", 3)

      .closeTag("xs:annotation", 2)

      .beginOpenTag("xs:restriction", 2).appendAttribute("base", "xs:time").endOpenTag()

      .beginOpenTag("xs:pattern", 3).appendAttribute("value", "\\d{2}:\\d{2}:\\d{2}Z?").endShorthandTag()

      .closeTag("xs:restriction", 2)

      .closeTag("xs:simpleType", 1);

    // xs:simpleType name="dateTimeType"
    xsdWriter
      .beginOpenTag("xs:simpleType", 1)
      .appendAttribute("name", "dateTimeType")
      .endOpenTag()

      .openTag("xs:annotation", 2)

      .openTag("xs:documentation", 3)
      .append(
        "dateTimeType restricts xs:dateTime to dates between 0001 and 9999 and is in UTC (no +/- after the T but an optional Z)")
      .closeTag("xs:documentation", 3)

      .closeTag("xs:annotation", 2)

      .beginOpenTag("xs:restriction", 2).appendAttribute("base", "xs:dateTime").endOpenTag()

      .beginOpenTag("xs:minInclusive", 3).appendAttribute("value", "0001-01-01T00:00:00.000000000Z").endShorthandTag()

      .beginOpenTag("xs:maxExclusive", 3).appendAttribute("value", "10000-01-01T00:00:00.000000000Z").endShorthandTag()

      .beginOpenTag("xs:pattern", 3).appendAttribute("value", "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d*)Z?")
      .endShorthandTag()

      .closeTag("xs:restriction", 2)

      .closeTag("xs:simpleType", 1);

    // close schema
    xsdWriter.closeTag("xs:schema");

    xsdWriter.close();
  }
}
