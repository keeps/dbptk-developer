/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.databasepreservation.common.InputStreamProviderImpl;
import com.databasepreservation.common.WaitingInputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.MessageDigestCalculatingInputStream;
import org.apache.commons.io.input.ObservableInputStream;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.TemporaryPathInputStreamProvider;
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
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

import javax.xml.bind.DatatypeConverter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentExportStrategy implements ContentExportStrategy {
  private final static String ENCODING = "UTF-8";
  private final static int THRESHOLD_TREAT_STRING_AS_CLOB = 4000;
  private final static int THRESHOLD_TREAT_BINARY_AS_BLOB = 2000;
  private final static String CELL_PREFIX_DEFAULT = "c";
  private final static String CELL_PREFIX_ARRAY = "a";
  private final static String CELL_PREFIX_UDT = "u";

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

  protected Reporter reporter;

  public SIARD2ContentExportStrategy(SIARD2ContentPathExportStrategy contentPathStrategy, WriteStrategy writeStrategy,
    SIARDArchiveContainer baseContainer, boolean prettyXMLOutput) {
    this.contentPathStrategy = contentPathStrategy;
    this.writeStrategy = writeStrategy;
    this.baseContainer = baseContainer;

    this.LOBsToExport = new ArrayList<>();
    currentRowIndex = -1;
    this.prettyXMLOutput = prettyXMLOutput;
  }

  @Override
  public void openSchema(SchemaStructure schema) {
    currentSchema = schema;
  }

  @Override
  public void closeSchema(SchemaStructure schema) {
    // do nothing
  }

  @Override
  public void openTable(TableStructure table) throws ModuleException {
    currentStream = writeStrategy.createOutputStream(baseContainer,
      contentPathStrategy.getTableXmlFilePath(currentSchema.getIndex(), table.getIndex()));
    currentWriter = new XMLBufferedWriter(currentStream, prettyXMLOutput);
    currentTable = table;
    currentRowIndex = 0;
    LOBsToExport = new ArrayList<>();

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
    try {
      if (!writeStrategy.isSimultaneousWritingSupported()) {
        for (LargeObject largeObject : LOBsToExport) {
          writeLOB(largeObject);
        }
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing LOBs").withCause(e);
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
          writeBinaryCell(CELL_PREFIX_DEFAULT, cell, column, columnIndex);
        } else if (cell instanceof SimpleCell) {
          writeSimpleCell(CELL_PREFIX_DEFAULT, cell, column, columnIndex);
        } else if (cell instanceof ArrayCell) {
          writeArrayCell(CELL_PREFIX_DEFAULT, cell, column, columnIndex);
        } else if (cell instanceof ComposedCell) {
          writeComposedCell(CELL_PREFIX_DEFAULT, cell, column, columnIndex);
        } else if (cell instanceof NullCell) {
          writeNullCell(CELL_PREFIX_DEFAULT, cell, column, columnIndex);
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

  private void writeArrayCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {

    ArrayCell arrayCell = (ArrayCell) cell;

    // currently open array-index tags in the XML
    List<Integer> tags = new ArrayList<>();

    // these two form a diff between the tags used in the last cell and the tags
    // that need to exist for the next cell
    Deque<Integer> tagsToOpen, tagsToClose;

    currentWriter.openTag(cellPrefix + columnIndex, 2);
    for (Pair<List<Integer>, Cell> listCellPair : arrayCell) {
      List<Integer> indexes = listCellPair.getLeft();
      Cell subCell = listCellPair.getRight();

      // get a diff between the last indexes and the new indexes. tagsToOpen will be
      // the "additions" part of the diff, while tagsToClose will be the "removals"
      // part of the diff.

      tagsToOpen = new LinkedList<>(indexes);
      for (Integer alreadyOpened : tags) {
        if (!tagsToOpen.isEmpty() && tagsToOpen.peek().equals(alreadyOpened)) {
          tagsToOpen.pop();
        } else {
          break;
        }
      }

      tagsToClose = new LinkedList<>(tags);
      for (Integer newIndexThatMayBeAlreadyOpened : indexes) {
        if (!tagsToClose.isEmpty() && tagsToClose.peek().equals(newIndexThatMayBeAlreadyOpened)) {
          tagsToClose.pop();
        } else {
          break;
        }
      }

      // close the tags that need to be closed, starting with the innermost
      Iterator<Integer> tagsToCloseReverseIterator = tagsToClose.descendingIterator();
      while (tagsToCloseReverseIterator.hasNext()) {
        currentWriter.closeTag(CELL_PREFIX_ARRAY + tagsToCloseReverseIterator.next(), 2);
      }

      // update current tags tracker
      tags = tags.subList(0, tags.size() - tagsToClose.size());

      // open the new tags
      Integer cellTag = tagsToOpen.pollLast();
      for (Integer index : tagsToOpen) {
        currentWriter.openTag(CELL_PREFIX_ARRAY + index, 2);
        tags.add(index);
      }

      if (subCell instanceof BinaryCell) {
        writeBinaryCell(CELL_PREFIX_ARRAY, subCell, column, cellTag);
      } else if (subCell instanceof SimpleCell) {
        writeSimpleCell(CELL_PREFIX_ARRAY, subCell, column, cellTag);
      } else if (subCell instanceof ComposedCell) {
        writeComposedCell(CELL_PREFIX_ARRAY, subCell, column, cellTag);
      } else if (subCell instanceof NullCell) {
        writeNullCell(CELL_PREFIX_ARRAY, subCell, column, cellTag);
      } else if (subCell instanceof ArrayCell) {
        writeArrayCell(CELL_PREFIX_ARRAY, subCell, column, cellTag);
      }
    }

    // close all array tags
    Collections.reverse(tags);
    for (Integer index : tags) {
      currentWriter.closeTag(CELL_PREFIX_ARRAY + index, 2);
    }

    currentWriter.closeTag(cellPrefix + columnIndex, 2);

  }

  private void writeComposedCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {

    ComposedCell composedCell = (ComposedCell) cell;

    currentWriter.openTag(cellPrefix + columnIndex, 2);

    int subCellIndex = 1;
    for (Cell subCell : composedCell.getComposedData()) {
      if (subCell == null || subCell instanceof NullCell) {
        // silently ignore
      } else if (subCell instanceof SimpleCell) {
        SimpleCell simpleCell = (SimpleCell) subCell;
        if (simpleCell.getSimpleData() != null) {
          currentWriter.inlineOpenTag(CELL_PREFIX_UDT + subCellIndex, 3);
          currentWriter.write(XMLUtils.encode(simpleCell.getSimpleData()));
          currentWriter.closeTag(CELL_PREFIX_UDT + subCellIndex);
        }
      } else if (subCell instanceof ComposedCell) {
        // currentWriter.inlineOpenTag(CELL_PREFIX_UDT + subCellIndex, 3);
        // currentWriter.closeTag(CELL_PREFIX_UDT + subCellIndex);

        LOGGER.warn("UDT inside UDT not yet supported. Saving as null.");
      } else if (subCell instanceof BinaryCell) {
        // currentWriter.inlineOpenTag(CELL_PREFIX_UDT + subCellIndex, 3);
        // currentWriter.closeTag(CELL_PREFIX_UDT + subCellIndex);

        LOGGER.warn("LOBs inside UDT not yet supported. Saving as null.");
      } else if (subCell instanceof ArrayCell) {
        // currentWriter.inlineOpenTag(CELL_PREFIX_UDT + subCellIndex, 3);
        // currentWriter.closeTag(CELL_PREFIX_UDT + subCellIndex);

        LOGGER.warn("Arrays inside UDT not yet supported. Saving as null.");
      } else {
        LOGGER.error("Unexpected cell type");
      }

      subCellIndex++;
    }

    currentWriter.closeTag(cellPrefix + columnIndex, 2);
  }

  protected void writeNullCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    NullCell nullCell = (NullCell) cell;
    writeNullCellData(cellPrefix, nullCell, columnIndex);
  }

  protected void writeSimpleCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    SimpleCell simpleCell = (SimpleCell) cell;

    if (Sql2008toXSDType.isLargeType(column.getType(), reporter)
      && simpleCell.getBytesSize() > THRESHOLD_TREAT_STRING_AS_CLOB) {
      writeLargeObjectData(cellPrefix, cell, columnIndex);
    } else {
      writeSimpleCellData(cellPrefix, simpleCell, columnIndex);
    }
  }

  protected void writeBinaryCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    BinaryCell binaryCell = (BinaryCell) cell;

    long length = binaryCell.getSize();
    if (length <= 0) {
      NullCell nullCell = new NullCell(binaryCell.getId());
      writeNullCellData(cellPrefix, nullCell, columnIndex);
      binaryCell.cleanResources();
    } else if (Sql2008toXSDType.isLargeType(column.getType(), reporter) && length > THRESHOLD_TREAT_BINARY_AS_BLOB) {
      writeLargeObjectData(cellPrefix, cell, columnIndex);
    } else {
      // inline binary data
      InputStream inputStream = binaryCell.createInputStream();
      byte[] bytes = IOUtils.toByteArray(inputStream);
      IOUtils.closeQuietly(inputStream);
      binaryCell.cleanResources();
      SimpleCell simpleCell = new SimpleCell(binaryCell.getId(), Hex.encodeHexString(bytes));
      writeSimpleCellData(cellPrefix, simpleCell, columnIndex);
    }
  }

  protected void writeNullCellData(String cellPrefix, NullCell nullcell, int columnIndex) throws IOException {
    // do nothing, as null cells are simply omitted
  }

  protected void writeSimpleCellData(String cellPrefix, SimpleCell simpleCell, int columnIndex) throws IOException {
    if (simpleCell.getSimpleData() != null) {
      currentWriter.inlineOpenTag(cellPrefix + columnIndex, 2);
      currentWriter.write(XMLUtils.encode(simpleCell.getSimpleData()));
      currentWriter.closeTag(cellPrefix + columnIndex);
    }
  }

  protected void writeLargeObjectData(String cellPrefix, Cell cell, int columnIndex)
    throws IOException, ModuleException {

    LargeObject lob = null;

    if (cell instanceof BinaryCell) {
      final BinaryCell binCell = (BinaryCell) cell;

      InputStream inputStream = null;
      try {

        MessageDigestCalculatingInputStream digest = new MessageDigestCalculatingInputStream(binCell.createInputStream(), MessageDigest.getInstance("MD5"));
        final WaitingInputStream waitingInputStream = new WaitingInputStream(digest);
        inputStream = new BufferedInputStream(waitingInputStream);

        lob = new LargeObject(new InputStreamProviderImpl(inputStream), contentPathStrategy
            .getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex, currentRowIndex + 1));

        writeLOB(lob);

        // wait for lob to be consumed so digest is calculated
        waitingInputStream.waitForClose();

        currentWriter.beginOpenTag(cellPrefix + columnIndex, 2).space().append("file=\"")
            .append(contentPathStrategy.getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
                currentRowIndex + 1))
            .append('"').space().append("length=\"").append(String.valueOf(binCell.getSize())).append("\"").space()
            .append("digest=\"").append(DatatypeConverter.printHexBinary(digest.getMessageDigest().digest()).toUpperCase()).append("\"").space()
            .append("digestType=\"").append("MD5").append("\"");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;

      // workaround to have data from CLOBs saved as a temporary file to be read
      String data = txtCell.getSimpleData();

      if (txtCell.getBytesSize() < 0) {
        // NULL content
        writeNullCellData(cellPrefix, new NullCell(cell.getId()), columnIndex);
        return;
      }

      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data.getBytes());
      try {
        MessageDigestCalculatingInputStream digest = new MessageDigestCalculatingInputStream(byteArrayInputStream, MessageDigest.getInstance("MD5"));
        final WaitingInputStream waitingInputStream = new WaitingInputStream(digest);
        InputStream inputStream = new BufferedInputStream(waitingInputStream);

        lob = new LargeObject(new InputStreamProviderImpl(inputStream), contentPathStrategy
            .getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex, currentRowIndex + 1));

        writeLOB(lob);

        // wait for lob to be consumed so digest is calculated
        waitingInputStream.waitForClose();

        currentWriter.beginOpenTag(cellPrefix + columnIndex, 2).space().append("file=\"")
            .append(contentPathStrategy.getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(), columnIndex,
                currentRowIndex + 1))
            .append('"').space().append("length=\"").append(String.valueOf(txtCell.getBytesSize())).append("\"").space()
            .append("digest=\"").append(DatatypeConverter.printHexBinary(digest.getMessageDigest().digest()).toUpperCase()).append("\"").space()
            .append("digestType=\"").append("MD5").append("\"");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    }

    // decide to whether write the LOB right away or later
    //if (writeStrategy.isSimultaneousWritingSupported()) {
    //  writeLOB(lob);
    //} else {
    //  LOBsToExport.add(lob);
    //}

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

  protected void writeLOB(LargeObject lob) throws ModuleException, IOException {
    LOGGER.debug("Writing lob to {}", lob.getOutputPath());

    InputStream in = null;
    // copy lob to output

    writeStrategy.writeTo(lob.getInputStreamProvider(), lob.getOutputPath());

//    try (OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getOutputPath())) {
//      in = lob.getInputStreamProvider().createInputStream();
//      IOUtils.copy(in, out);
//      in.close();
//    } catch (IOException e) {
//      throw new ModuleException().withMessage("Could not write lob").withCause(e);
//    } finally {
//      lob.getInputStreamProvider().cleanResources();
//    }
  }

  private void writeXsd() throws IOException, ModuleException {
    OutputStream xsdStream = writeStrategy.createOutputStream(baseContainer,
      contentPathStrategy.getTableXsdFilePath(currentSchema.getIndex(), currentTable.getIndex()));
    XMLBufferedWriter xsdWriter = new XMLBufferedWriter(xsdStream, prettyXMLOutput);

    xsdWriter
      // ?xml tag
      .append("<?xml version=\"1.0\" encoding=\"").append(ENCODING).append("\" standalone=\"no\"?>").newline()

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
      .appendAttribute("name", "row").appendAttribute("type", "recordType").endShorthandTag()

      .closeTag("xs:sequence", 3)

      .closeTag("xs:complexType", 2)

      .closeTag("xs:element", 1)

      // xs:complexType name="rowType"
      .beginOpenTag("xs:complexType", 1).appendAttribute("name", "recordType").endOpenTag()

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

        xsdWriter.beginOpenTag("xs:any", 7).appendAttribute("minOccurs", "0").appendAttribute("maxOccurs", "unbounded")
          .appendAttribute("processContents", "skip").endShorthandTag();

        // </xs:sequence>
        xsdWriter.closeTag("xs:sequence", 6);

        // </xs:complexType>
        xsdWriter.closeTag("xs:complexType", 5);

        xsdWriter.closeTag("xs:element", 4);
      } else {
        try {
          String xsdType = Sql2008toXSDType.convert(col.getType(), reporter);

          xsdWriter.beginOpenTag("xs:element", 4);

          if (col.isNillable()) {
            xsdWriter.appendAttribute("minOccurs", "0");
          }

          xsdWriter.appendAttribute("name", "c" + columnIndex).appendAttribute("type", xsdType).endShorthandTag();
        } catch (ModuleException e) {
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

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "digestType").appendAttribute("type", "digestTypeType")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "digest").appendAttribute("type", "xs:string")
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

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "digestType").appendAttribute("type", "digestTypeType")
      .endShorthandTag()

      .beginOpenTag("xs:attribute", 4).appendAttribute("name", "digest").appendAttribute("type", "xs:string")
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
    xsdWriter.beginOpenTag("xs:simpleType", 1).appendAttribute("name", "dateTimeType").endOpenTag()

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

    xsdWriter.beginOpenTag("xs:simpleType", 1).appendAttribute("name", "digestTypeType").endOpenTag()

      .beginOpenTag("xs:restriction", 2).appendAttribute("base", "xs:string").endOpenTag()

      .beginOpenTag("xs:whiteSpace", 3).appendAttribute("value", "collapse").endShorthandTag()

      .beginOpenTag("xs:enumeration", 3).appendAttribute("value", "MD5").endShorthandTag()

      .beginOpenTag("xs:enumeration", 3).appendAttribute("value", "SHA-1").endShorthandTag()

      .beginOpenTag("xs:enumeration", 3).appendAttribute("value", "SHA-256").endShorthandTag()

      .closeTag("xs:restriction", 2)

      .closeTag("xs:simpleType", 1);

    // close schema
    xsdWriter.closeTag("xs:schema");

    xsdWriter.close();
  }
}
