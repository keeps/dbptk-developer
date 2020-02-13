/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.providers.TemporaryPathInputStreamProvider;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.ArrayCell;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
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
  private final static int THRESHOLD_TREAT_STRING_AS_CLOB = 4000;
  private final static int THRESHOLD_TREAT_BINARY_AS_BLOB = 2000;

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD1ContentExportStrategy.class);
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
  boolean warnedAboutUDT = false;

  private Reporter reporter;

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
      throw new ModuleException().withMessage("Error handling open table " + table.getId()).withCause(e);
    }
  }

  @Override
  public void closeTable(TableStructure table) throws ModuleException {
    // finish writing the table
    try {
      writeXmlCloseTable();
      currentWriter.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error handling close table " + table.getId()).withCause(e);
    }

    // write lobs if they have not been written yet
    if (!writeStrategy.isSimultaneousWritingSupported()) {
      for (LargeObject largeObject : LOBsToExport) {
        writeLOB(largeObject);
      }
    }

    // export table XSD
    try {
      writeXsd();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing table XSD").withCause(e);
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
        } else if (cell instanceof ArrayCell) {
          reporter.cellProcessingUsedNull(currentTable, column, currentRowIndex,
            new ModuleException().withMessage("SIARD 1 does not support arrays."));
        } else if (cell instanceof ComposedCell) {
          writeComposedCell(cell, column, columnIndex);
        } else if (cell instanceof NullCell) {
          writeNullCell(cell, column, columnIndex);
        }
      }

      currentWriter.closeTag("row", 1);
      currentRowIndex++;
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write row" + row.toString()).withCause(e);
    }
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  private void writeComposedCell(Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    if (!warnedAboutUDT) {
      warnedAboutUDT = true;
      LOGGER.warn("User Defined Types are not supported in SIARD1");
    }

  }

  private void writeNullCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    NullCell nullCell = (NullCell) cell;
    writeNullCellData(nullCell, columnIndex);
  }

  private void writeSimpleCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    SimpleCell simpleCell = (SimpleCell) cell;

    if (Sql99toXSDType.isLargeType(column.getType(), reporter)
      && simpleCell.getBytesSize() > THRESHOLD_TREAT_STRING_AS_CLOB) {
      writeLargeObjectData(cell, columnIndex);
    } else {
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  private void writeBinaryCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException {
    BinaryCell binaryCell = (BinaryCell) cell;

    long length = binaryCell.getSize();
    if (length == 0) {
      // TODO: make sure this never happens
      NullCell nullCell = new NullCell(binaryCell.getId());
      writeNullCellData(nullCell, columnIndex);
    } else if (Sql99toXSDType.isLargeType(column.getType(), reporter) && length > THRESHOLD_TREAT_BINARY_AS_BLOB) {
      writeLargeObjectData(cell, columnIndex);
    } else {
      // inline binary data
      InputStream inputStream = binaryCell.createInputStream();
      byte[] bytes = IOUtils.toByteArray(inputStream);
      IOUtils.closeQuietly(inputStream);
      SimpleCell simpleCell = new SimpleCell(binaryCell.getId(), Hex.encodeHexString(bytes));
      writeSimpleCellData(simpleCell, columnIndex);
    }
  }

  private void writeNullCellData(NullCell nullcell, int columnIndex) throws IOException {
    // do nothing, as null cells are simply omitted
  }

  private void writeSimpleCellData(SimpleCell simpleCell, int columnIndex) throws IOException {
    if (simpleCell.getSimpleData() != null) {
      currentWriter.inlineOpenTag("c" + columnIndex, 2);
      currentWriter.write(XMLUtils.encode(simpleCell.getSimpleData()));
      currentWriter.closeTag("c" + columnIndex);
    }
  }

  private void writeLargeObjectData(Cell cell, int columnIndex) throws IOException, ModuleException {
    LargeObject lob = null;

    if (cell instanceof BinaryCell) {
      final BinaryCell binCell = (BinaryCell) cell;

      String path = contentPathStrategy.getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
        currentRowIndex + 1);

      lob = new LargeObject(binCell, path);

      currentWriter.beginOpenTag("c" + columnIndex, 2).space().append("file=\"").append(path).append('"').space()
        .append("length=\"").append(String.valueOf(binCell.getSize())).append("\"");
    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;

      if (txtCell.getBytesSize() < 0) {
        // NULL content
        writeNullCellData(new NullCell(txtCell.getId()), columnIndex);
        return;
      }

      String path = contentPathStrategy.getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
        currentRowIndex + 1);

      // workaround to have data from CLOBs saved as a temporary file to be read
      String data = txtCell.getSimpleData();
      if (data == null) {
        writeNullCellData(new NullCell(cell.getId()), columnIndex);
        return;
      }

      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes());
      lob = new LargeObject(new TemporaryPathInputStreamProvider(inputStream), path);

      currentWriter.beginOpenTag("c" + columnIndex, 2).space().append("file=\"").append(path).append('"').space()
        .append("length=\"").append(String.valueOf(txtCell.getBytesSize())).append("\"");
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
    currentWriter.append("<?xml version=\"1.0\" encoding=\"").append(ENCODING).append("\"?>").newline()

      .beginOpenTag("table", 0)

      .appendAttribute("xsi:schemaLocation",
        contentPathStrategy.getTableXsdNamespace(
          "http://www.admin.ch/xmlns/siard/" + baseContainer.getVersion().getNamespace() + "/",
          currentSchema.getIndex(), currentTable.getIndex()) + " "
          + contentPathStrategy.getTableXsdFileName(currentTable.getIndex()))

      .appendAttribute("xmlns",
        contentPathStrategy.getTableXsdNamespace(
          "http://www.admin.ch/xmlns/siard/" + baseContainer.getVersion().getNamespace() + "/",
          currentSchema.getIndex(), currentTable.getIndex()))

      .appendAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")

      .endOpenTag();
  }

  private void writeXmlCloseTable() throws IOException {
    currentWriter.closeTag("table", 0);
  }

  private void writeLOB(LargeObject lob) throws ModuleException {
    OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getOutputPath());
    InputStream in = lob.getInputStreamProvider().createInputStream();

    // copy lob to output
    try {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write lob").withCause(e);
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
      .append("<?xml version=\"1.0\" encoding=\"").append(ENCODING).append("\"?>").newline()

      // xs:schema tag
      .beginOpenTag("xs:schema", 0).appendAttribute("xmlns:xs", "http://www.w3.org/2001/XMLSchema")

      .appendAttribute("xmlns",
        contentPathStrategy.getTableXsdNamespace(
          "http://www.admin.ch/xmlns/siard/" + baseContainer.getVersion().getNamespace() + "/",
          currentSchema.getIndex(), currentTable.getIndex()))

      .appendAttribute("attributeFormDefault", "unqualified")

      .appendAttribute("elementFormDefault", "qualified")

      .appendAttribute("targetNamespace",
        contentPathStrategy.getTableXsdNamespace(
          "http://www.admin.ch/xmlns/siard/" + baseContainer.getVersion().getNamespace() + "/",
          currentSchema.getIndex(), currentTable.getIndex()))

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
        String xsdType = Sql99toXSDType.convert(col.getType(), reporter);

        xsdWriter.beginOpenTag("xs:element", 4);

        if (col.isNillable()) {
          xsdWriter.appendAttribute("minOccurs", "0");
        }

        xsdWriter.appendAttribute("name", "c" + columnIndex).appendAttribute("type", xsdType).endShorthandTag();
      } catch (ModuleException e) {
        LOGGER.error(String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
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

    // close schema
    xsdWriter.closeTag("xs:schema");

    xsdWriter.close();
  }

}
