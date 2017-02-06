package com.databasepreservation.modules.siard.out.content;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.TemporaryPathInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentExportStrategy implements ContentExportStrategy {
  private final static String ENCODING = "UTF-8";
  private final static int THRESHOLD_TREAT_STRING_AS_CLOB = 4000;
  private final static int THRESHOLD_TREAT_BINARY_AS_BLOB = 2000;

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD2ContentExportStrategy.class);
  protected final SIARD2ContentPathExportStrategy contentPathStrategy;
  protected final WriteStrategy writeStrategy;
  protected final SIARDArchiveContainer baseContainer;
  private final boolean prettyXMLOutput;
  XMLBufferedWriter currentWriter;
  OutputStream currentStream;
  SchemaStructure currentSchema;
  TableStructure currentTable;
  int currentRowIndex;
  private List<LargeObject> LOBsToExport;

  // <columnId, maxLength>
  private Map<String, Integer> arrayMaximumLength;

  public SIARD2ContentExportStrategy(SIARD2ContentPathExportStrategy contentPathStrategy, WriteStrategy writeStrategy,
    SIARDArchiveContainer baseContainer, boolean prettyXMLOutput) {
    this.contentPathStrategy = contentPathStrategy;
    this.writeStrategy = writeStrategy;
    this.baseContainer = baseContainer;

    this.LOBsToExport = new ArrayList<LargeObject>();
    currentRowIndex = -1;
    this.prettyXMLOutput = prettyXMLOutput;
    arrayMaximumLength = new HashMap<>();
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
        } else if (cell instanceof NullCell) {
          writeNullCell(cell, column, columnIndex);
        }
      }

      currentWriter.closeTag("row", 1);
      currentRowIndex++;
    } catch (IOException e) {
      throw new ModuleException("Could not write row" + row.toString(), e);
    }
  }

  private void writeComposedCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException,
    IOException {

    ComposedCell composedCell = (ComposedCell) cell;

    String cellPrefix = "";
    if (column.getType() instanceof ComposedTypeArray) {
      cellPrefix = "a";
    } else if (column.getType() instanceof ComposedTypeStructure) {
      cellPrefix = "u";
    }

    currentWriter.openTag("c" + columnIndex, 2);

    int subCellIndex = 1;
    for (Cell subCell : composedCell.getComposedData()) {
      if (subCell == null || subCell instanceof NullCell) {
        // silently ignore
      } else if (subCell instanceof SimpleCell) {
        SimpleCell simpleCell = (SimpleCell) subCell;
        if (simpleCell.getSimpleData() != null) {
          currentWriter.inlineOpenTag(cellPrefix + subCellIndex, 3);
          currentWriter.write(XMLUtils.encode(simpleCell.getSimpleData()));
          currentWriter.closeTag(cellPrefix + subCellIndex);
        }
      } else if (subCell instanceof ComposedCell) {
        // currentWriter.inlineOpenTag("u" + subCellIndex, 3);
        // currentWriter.closeTag("u" + subCellIndex);

        LOGGER.warn("UDT inside UDT not yet supported. Saving as null.");
      } else if (subCell instanceof BinaryCell) {
        // currentWriter.inlineOpenTag("u" + subCellIndex, 3);
        // currentWriter.closeTag("u" + subCellIndex);

        LOGGER.warn("LOBs inside UDT not yet supported. Saving as null.");
      } else {
        LOGGER.error("Unexpected cell type");
      }

      subCellIndex++;
    }

    // update information about the maximum length of the array
    if (column.getType() instanceof ComposedTypeArray) {
      int newMax = subCellIndex - 1;
      Integer max = arrayMaximumLength.get(column.getId());
      if (max == null || max < newMax) {
        arrayMaximumLength.put(column.getId(), newMax);
      }
    }

    currentWriter.closeTag("c" + columnIndex, 2);
  }

  protected void writeNullCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    NullCell nullCell = (NullCell) cell;
    writeNullCellData(nullCell, columnIndex);
  }

  protected void writeSimpleCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException,
    IOException {
    SimpleCell simpleCell = (SimpleCell) cell;

    if (Sql2008toXSDType.isLargeType(column.getType()) && simpleCell.getBytesSize() > THRESHOLD_TREAT_STRING_AS_CLOB) {
      writeLargeObjectData(cell, columnIndex);
    } else {
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  protected void writeBinaryCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException,
    IOException {
    BinaryCell binaryCell = (BinaryCell) cell;

    long length = binaryCell.getSize();
    if (length <= 0) {
      NullCell nullCell = new NullCell(binaryCell.getId());
      writeNullCellData(nullCell, columnIndex);
      binaryCell.cleanResources();
    } else if (Sql2008toXSDType.isLargeType(column.getType()) && length > THRESHOLD_TREAT_BINARY_AS_BLOB) {
      writeLargeObjectData(cell, columnIndex);
    } else {
      // inline binary data
      InputStream inputStream = binaryCell.createInputStream();
      byte[] bytes = IOUtils.toByteArray(inputStream);
      IOUtils.closeQuietly(inputStream);
      binaryCell.cleanResources();
      SimpleCell simpleCell = new SimpleCell(binaryCell.getId(), Hex.encodeHexString(bytes));
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  protected void writeNullCellData(NullCell nullcell, int columnIndex) throws IOException {
    // do nothing, as null cells are simply omitted
  }

  protected void writeSimpleCellData(SimpleCell simpleCell, int columnIndex) throws IOException {
    if (simpleCell.getSimpleData() != null) {
      currentWriter.inlineOpenTag("c" + columnIndex, 2);
      currentWriter.write(XMLUtils.encode(simpleCell.getSimpleData()));
      currentWriter.closeTag("c" + columnIndex);
    }
  }

  protected void writeLargeObjectData(Cell cell, int columnIndex) throws IOException, ModuleException {

    LargeObject lob = null;

    if (cell instanceof BinaryCell) {
      final BinaryCell binCell = (BinaryCell) cell;

      lob = new LargeObject(binCell, contentPathStrategy.getBlobFilePath(currentSchema.getIndex(),
        currentTable.getIndex(), columnIndex, currentRowIndex + 1));

      currentWriter.beginOpenTag("c" + columnIndex, 2).space().append("file=\"")
        .append(contentPathStrategy.getBlobFileName(currentRowIndex + 1)).append('"').space().append("length=\"")
        .append(String.valueOf(binCell.getSize())).append("\"");

    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;

      // workaround to have data from CLOBs saved as a temporary file to be read
      String data = txtCell.getSimpleData();

      if (txtCell.getBytesSize() < 0) {
        // NULL content
        writeNullCellData(new NullCell(cell.getId()), columnIndex);
        return;
      }

      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes());
      lob = new LargeObject(new TemporaryPathInputStreamProvider(inputStream), contentPathStrategy.getClobFilePath(
        currentSchema.getIndex(), currentTable.getIndex(), columnIndex, currentRowIndex + 1));

      currentWriter.beginOpenTag("c" + columnIndex, 2).space().append("file=\"")
        .append(contentPathStrategy.getClobFileName(currentRowIndex + 1)).append('"').space().append("length=\"")
        .append(String.valueOf(txtCell.getBytesSize())).append("\"");
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

  protected void writeLOB(LargeObject lob) throws ModuleException, IOException {
    OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getOutputPath());
    InputStream in = lob.getInputStreamProvider().createInputStream();

    LOGGER.debug("Writing lob to " + lob.getOutputPath());

    // copy lob to output
    try {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new ModuleException("Could not write lob", e);
    } finally {
      // close resources
      IOUtils.closeQuietly(in);
      IOUtils.closeQuietly(out);
      lob.getInputStreamProvider().cleanResources();
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
      if (col.getType() instanceof ComposedTypeStructure) {
        Type composedType = (ComposedTypeStructure) col.getType();

        // FIXME: if the same table contains two columns of different UDTs which
        // subtypes differ then there would exist conflicting definitions for
        // elements u1, u2, u3, etc
        LOGGER.warn("XSD validation of tables containing UDT is not yet supported.");
      } else if (col.getType() instanceof ComposedTypeArray) {
        ComposedTypeArray arrayType = (ComposedTypeArray) col.getType();

        // <xs:element minOccurs="0" name="c4">
        xsdWriter.beginOpenTag("xs:element", 4);
        if (col.isNillable()) {
          xsdWriter.appendAttribute("minOccurs", "0");
        }
        xsdWriter.appendAttribute("name", "c" + columnIndex).endOpenTag();

        // <xs:complexType>
        xsdWriter.openTag("xs:complexType", 5);

        // <xs:sequence>
        xsdWriter.openTag("xs:sequence", 6);

        String xsdType = null;
        try {
          xsdType = Sql2008toXSDType.convert(arrayType.getElementType());
        } catch (UnknownTypeException e) {
          LOGGER.error(
            String.format("An error occurred while getting the XSD subtype of array column c%d", columnIndex), e);
        }

        Integer maxLength = arrayMaximumLength.get(col.getId());

        if (xsdType != null && maxLength != null) {
          for (int i = 1; i <= maxLength; i++) {
            xsdWriter.beginOpenTag("xs:element", 7).appendAttribute("minOccurs", "0");
            xsdWriter.appendAttribute("name", "a" + i).appendAttribute("type", xsdType).endShorthandTag();
          }
        }

        // </xs:sequence>
        xsdWriter.closeTag("xs:sequence", 6);

        // </xs:complexType>
        xsdWriter.closeTag("xs:complexType", 5);

        xsdWriter.closeTag("xs:element", 4);
      } else {
        try {
          String xsdType = Sql2008toXSDType.convert(col.getType());

          xsdWriter.beginOpenTag("xs:element", 4);

          if (col.isNillable()) {
            xsdWriter.appendAttribute("minOccurs", "0");
          }

          xsdWriter.appendAttribute("name", "c" + columnIndex).appendAttribute("type", xsdType).endShorthandTag();
        } catch (ModuleException e) {
          LOGGER.error(String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
        } catch (UnknownTypeException e) {
          LOGGER.error(String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
        }
      }
      columnIndex++;
    }

    xsdWriter
    // close tags for xs:sequence and xs:complexType
      .closeTag("xs:sequence", 2)

      .closeTag("xs:complexType", 1);

    // xs:complexType name="clobType"
    xsdWriter.beginOpenTag("xs:complexType", 1).appendAttribute("name", "clobType").endOpenTag()

    .openTag("xs:annotation", 2)

    .inlineOpenTag("xs:documentation", 3).append("Type to refer CLOB types. Either inline or in a separate file.")
      .closeTag("xs:documentation")

      .closeTag("xs:annotation", 2)

      .openTag("xs:simpleContent", 2)

      .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:string").endOpenTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:anyURI")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "messageDigest").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1);

    // xs:complexType name="blobType"
    xsdWriter.beginOpenTag("xs:complexType", 1).appendAttribute("name", "blobType").endOpenTag()

    .openTag("xs:annotation", 2)

    .inlineOpenTag("xs:documentation", 3).append("Type to refer BLOB types. Either inline or in a separate file.")
      .closeTag("xs:documentation")

      .closeTag("xs:annotation", 2)

      .openTag("xs:simpleContent", 2)

      .beginOpenTag("xs:extension", 3).appendAttribute("base", "xs:hexBinary").endOpenTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "file").appendAttribute("type", "xs:anyURI")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "length").appendAttribute("type", "xs:integer")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "messageDigest").appendAttribute("type", "xs:string")
      .endShorthandTag()

      .closeTag("xs:extension", 3)

      .closeTag("xs:simpleContent", 2)

      .closeTag("xs:complexType", 1);

    // xs:simpleType name="dateType"
    xsdWriter.beginOpenTag("xs:simpleType", 1).appendAttribute("name", "dateType").endOpenTag()

    .openTag("xs:annotation", 2)

    .inlineOpenTag("xs:documentation", 3)
      .append("dateType restricts xs:date to dates between 0001 and 9999 and is in UTC (no +/- but an optional Z)")
      .closeTag("xs:documentation")

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

    .inlineOpenTag("xs:documentation", 3)
      .append("timeType restricts xs:date to dates between 0001 and 9999 and is in UTC (no +/- but an optional Z)")
      .closeTag("xs:documentation")

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

      .inlineOpenTag("xs:documentation", 3)
      .append(
        "dateTimeType restricts xs:dateTime to dates between 0001 and 9999 and is in UTC (no +/- after the T but an optional Z)")
      .closeTag("xs:documentation")

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
