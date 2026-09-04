/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.output.XMLOutputter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKDocIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.services.conversion.LobConversionAuditor;
import com.databasepreservation.modules.siard.services.conversion.model.report.ArtifactReport;
import com.databasepreservation.modules.siard.services.conversion.model.report.ConversionReport;
import com.databasepreservation.modules.siard.services.conversion.model.report.DbptkContext;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SIARDDKContentExportStrategy implements ContentExportStrategy {

  private static final String ENCODING = "utf-8";
  private static final String TAB = "  ";
  private static final String namespaceBase = "http://www.sa.dk/xmlns/siard/"
    + SIARDConstants.SiardVersion.DK.getNamespace() + "/";
  private static final Logger logger = LoggerFactory.getLogger(SIARDDKContentExportStrategy.class);

  private int tableCounter;
  private boolean foundClob;
  private boolean foundUnknownMimetype;

  private final ContentPathExportStrategy contentPathExportStrategy;
  private final SIARDDKFileIndexFileStrategy SIARDDKFileIndexFileStrategy;
  private final SIARDDKDocIndexFileStrategy SIARDDKDocIndexFileStrategy;
  private final SIARDArchiveContainer baseContainer;
  private OutputStream tableXmlOutputStream;
  private OutputStream tableXsdOutputStream;
  private BufferedWriter tableXmlWriter;
  private final WriteStrategy writeStrategy;
  private final LOBsTracker lobsTracker;
  private final MimetypeHandler mimetypeHandler;

  private final LobConversionAuditor auditor;
  private final ObjectMapper mapper;

  private Reporter reporter;

  public SIARDDKContentExportStrategy(SIARDDKExportModule siarddkExportModule) {

    tableCounter = 1;
    foundClob = false;
    foundUnknownMimetype = false;

    mimetypeHandler = new SIARDDKMimetypeHandler();

    contentPathExportStrategy = siarddkExportModule.getContentPathExportStrategy();
    SIARDDKFileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    SIARDDKDocIndexFileStrategy = siarddkExportModule.getDocIndexFileStrategy();
    baseContainer = siarddkExportModule.getMainContainer();
    writeStrategy = siarddkExportModule.getWriteStrategy();
    lobsTracker = siarddkExportModule.getLobsTracker();

    this.mapper = new ObjectMapper();
    Path exportRoot = baseContainer.getPath().getParent();
    String archiveName = baseContainer.getPath().getFileName().toString();
    this.auditor = new LobConversionAuditor(exportRoot, archiveName);
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

    tableXmlOutputStream = SIARDDKFileIndexFileStrategy.getWriter(baseContainer,
      contentPathExportStrategy.getTableXmlFilePath(0, tableCounter), writeStrategy);

    try {
      tableXmlWriter = new BufferedWriter(new OutputStreamWriter(tableXmlOutputStream, ENCODING));
    } catch (UnsupportedEncodingException e1) {
      throw new ModuleException().withCause(e1);
    }

    // Note: cannot use JAXB or JDOM to generate XML for tables, since the
    // actual tables are too large

    StringBuilder builder = new StringBuilder();
    builder.append("<?xml version=\"1.0\" encoding=\"").append(ENCODING).append("\"?>\n")

      .append("<table xsi:schemaLocation=\"")
      .append(contentPathExportStrategy.getTableXsdNamespace(namespaceBase, 0, tableCounter)).append(" ")
      .append(contentPathExportStrategy.getTableXsdFileName(tableCounter)).append("\" ").append("xmlns=\"")
      .append(contentPathExportStrategy.getTableXsdNamespace(namespaceBase, 0, tableCounter)).append("\" ")
      .append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"").append(">").append("\n");

    try {
      tableXmlWriter.write(builder.toString());
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error handling open table " + tableStructure.getId()).withCause(e);
    }

    // Code to write table XSDs - this has to happen before writing the table
    // rows due to the LOBsTracker

    // Set namespaces for schema
    Namespace defaultNamespace = Namespace
      .getNamespace(contentPathExportStrategy.getTableXsdNamespace(namespaceBase, 0, tableCounter));
    Namespace xs = Namespace.getNamespace("xs", "http://www.w3.org/2001/XMLSchema");

    // Create root element
    Element schema = new Element("schema", xs);
    schema.addNamespaceDeclaration(defaultNamespace);
    schema.setAttribute("targetNamespace",
      contentPathExportStrategy.getTableXsdNamespace(namespaceBase, 0, tableCounter));
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
      c.setAttribute("name", "c" + columnIndex);
      String sql99Type = columnStructure.getType().getSql99TypeName();

      // Register LOB in the LOBsTracker
      if (SIARDDKConstants.BINARY_LARGE_OBJECT.equals(sql99Type)
        || SIARDDKConstants.CHARACTER_LARGE_OBJECT.equals(sql99Type)) {
        lobsTracker.addLOBLocationAndType(tableCounter, columnIndex, sql99Type);
      }

      String xsdType = SIARDDKsql99ToXsdType.convert(sql99Type);
      if (xsdType == null) {
        throw new ModuleException().withMessage(
          "Unable to export column [" + columnStructure.getName() + "] in table [" + tableStructure.getName()
            + "], as siard-dk doesn't support the normalized SQL data type of the column: [" + sql99Type + "] ");
      }

      c.setAttribute("type", xsdType);
      if (columnStructure.isNillable()) {
        c.setAttribute("nillable", "true");
      }
      sequenceRowType.addContent(c);
      columnIndex++;
    }

    // Add elements to appropriate ancestors
    complexTypeRowType.addContent(sequenceRowType);
    schema.addContent(complexTypeRowType);

    // Write schema to archive

    // TO-DO: unfortunate name below: getLOBWriter (change the
    // FileIndexFileStrategy)
    tableXsdOutputStream = SIARDDKFileIndexFileStrategy.getLOBWriter(baseContainer,
      contentPathExportStrategy.getTableXsdFilePath(0, tableCounter), writeStrategy);
    BufferedWriter xsdWriter = new BufferedWriter(new OutputStreamWriter(tableXsdOutputStream));

    Document d = new Document(schema);
    XMLOutputter outputter = new XMLOutputter();
    try {
      // outputter.output(d, System.out);
      outputter.output(d, xsdWriter);
      xsdWriter.close();

      SIARDDKFileIndexFileStrategy.addFile(contentPathExportStrategy.getTableXsdFilePath(0, tableCounter));

    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write table" + tableCounter + " to disk").withCause(e);
    }
    foundUnknownMimetype = false;
  }

  @Override
  public void closeTable(TableStructure tableStructure) throws ModuleException {
    try {
      tableXmlWriter.write("</table>");
      tableXmlWriter.close();

      SIARDDKFileIndexFileStrategy.addFile(contentPathExportStrategy.getTableXmlFilePath(0, tableCounter));

      if (foundClob) {
        logger.info("CLOB(s) found in table " + tableCounter + ". Archived as string");
      }
      foundClob = false;
      tableCounter += 1;

    } catch (IOException e) {
      throw new ModuleException().withMessage("Error handling close table " + tableStructure.getId()).withCause(e);
    }

    if (foundUnknownMimetype) {
      String warning = new StringBuilder().append("Found BLOB(s) with unknown mimetype(s) in table '")
        .append(tableStructure.getName()).append("'. ").append("File(s) archived with extension '.bin'").toString();
      logger.warn(warning);
    }
  }

  @Override
  public Row tableRow(Row row) throws ModuleException {
    try {

      tableXmlWriter.append(TAB).append("<row>\n");

      int columnIndex = 0;
      for (Cell cell : row.getCells()) {
        columnIndex++;

        if (cell instanceof ComposedCell) {
          throw new ModuleException().withMessage("Cannot build composed cells yet");
        }

        if (lobsTracker.getLOBsType(tableCounter, columnIndex) == null) {
          // cell must be a SimpleCell since it is not registered in the
          // LOBsTracker

          if (!(cell instanceof NullCell)) {
            if (cell instanceof SimpleCell) {
              SimpleCell simpleCell = (SimpleCell) cell;
              // System.out.println("SimpleData = " +
              // simpleCell.getSimpleData());
              tableXmlWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
                .append(encodeText(simpleCell.getSimpleData())).append("</c").append(String.valueOf(columnIndex))
                .append(">\n");
            } else if (cell instanceof BinaryCell) {
              BinaryCell binaryCell = (BinaryCell) cell;
              InputStream in = binaryCell.createInputStream();

              tableXmlWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
                .append(Hex.encodeHexString(IOUtils.toByteArray(in))).append("</c").append(String.valueOf(columnIndex))
                .append(">\n");
              in.close();
              binaryCell.cleanResources();
            }
          } else {
            whiteNilCell(columnIndex);
          }

        } else {
          // cell must contain BLOB or CLOB

          if (cell instanceof NullCell) {
            whiteNilCell(columnIndex);
          } else if (cell instanceof SimpleCell) {

            // CLOB is not NULL

            // CLOB case - save as string
            // TO-DO: build case, where CLOB is archived as tiff

            SimpleCell simpleCell = (SimpleCell) cell;

            foundClob = true;
            String clobsData = simpleCell.getSimpleData().trim();
            lobsTracker.updateMaxClobLength(tableCounter, columnIndex, clobsData.length());

            // lobsTracker.addLOB(); // Only if LOB not NULL
            tableXmlWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
              .append(encodeText(clobsData)).append("</c").append(String.valueOf(columnIndex)).append(">\n");

          } else if (cell instanceof BinaryCell) {

            // BLOB case
            final BinaryCell binaryCell = (BinaryCell) cell;
            String mimeType = binaryCell.getMimeType() != null ? binaryCell.getMimeType() : "unsupported";

            // -------------------------------------------------------------
            // BLOB EXTRACTION DELEGATION
            // -------------------------------------------------------------
            if (mimeType.equals("application/zip")) {
              processConvertedLobArchive(binaryCell, row.getIndex(), columnIndex);
            } else {
              logger.warn(
                "Found BLOB with unsupported mimetype '{}' in table {}, column {}. ignoring content and archiving as .bin file.",
                mimeType, tableCounter, columnIndex);
              whiteNilCell(columnIndex);
            }
          } else {
            // never happens
          }
        }
      }

      tableXmlWriter.append(TAB).append("</row>\n");

    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write row " + row.toString()).withCause(e);
    }

    return row;
  }

  private void processConvertedLobArchive(BinaryCell binaryCell, long rowIndex, int columnIndex)
    throws ModuleException {
    try {
      ConversionReport report = extractReportFromZip(binaryCell);
      if (report == null) {
        throw new ModuleException().withMessage("Missing conversion_report.json in cell archive.");
      }

      String fileFromCell = binaryCell.getFile();
      if (fileFromCell != null) {
        String filename = FilenameUtils.getName(fileFromCell).stripTrailing();
        report = report.withOriginalFilename(filename);
      }

      List<String> siardPhysicalPaths = new ArrayList<>();
      int fileCount = 0;
      String processedFilesExtension = "tif";

      try (ZipInputStream zis = new ZipInputStream(binaryCell.createInputStream())) {
        ZipEntry zipEntry;
        while ((zipEntry = zis.getNextEntry()) != null) {
          if (zipEntry.getName().toLowerCase().contains("report"))
            continue;

          ArtifactReport artifactMeta = findArtifactMetadata(report.artifacts(), zipEntry.getName());

          // Check if this file is bypassed
          if (artifactMeta == null || artifactMeta.isBypassed()) {
            logger.warn("Ignoring bypassed or unknown file: {}. Reason: {}", zipEntry.getName(),
              artifactMeta != null ? artifactMeta.errorMessage() : "Not in report");
            continue;
          }

          // LOB isn't bypassed; add it to tracker now
          if (fileCount == 0) {
            double lobSizeTotal = ((double) binaryCell.getSize()) / (1024 * 1024);
            lobsTracker.addLOB(lobSizeTotal);
          }

          String fileExt = mimetypeHandler.getFileExtension(artifactMeta.finalMimeType());
          processedFilesExtension = fileExt;
          fileCount++;

          String outputPath = writeLobToSiardStorage(zis, fileCount, fileExt);
          siardPhysicalPaths.add(outputPath);
        }
      }

      if (fileCount > 0) {
        writeLobReferenceToXml(columnIndex);
        SIARDDKDocIndexFileStrategy.addDoc(lobsTracker.getLOBsCount(), 0, 1, lobsTracker.getDocCollectionCount(),
          report.originalFilename(), processedFilesExtension, null);
      } else {
        whiteNilCell(columnIndex);
      }

      ConversionReport enrichedReport = report
        .withContext(new DbptkContext(tableCounter, rowIndex, columnIndex, siardPhysicalPaths));
      auditor.appendAuditRecord(enrichedReport);

    } catch (Exception e) {
      throw new ModuleException().withMessage("Failed to process converted ZIP archive").withCause(e);
    }
  }

  private ConversionReport extractReportFromZip(BinaryCell binaryCell) throws Exception {
    try (ZipInputStream zis = new ZipInputStream(binaryCell.createInputStream())) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().toLowerCase().contains("report")) {
          return mapper.readValue(zis.readAllBytes(), ConversionReport.class);
        }
      }
    }
    return null;
  }

  private ArtifactReport findArtifactMetadata(List<ArtifactReport> artifacts, String fileName) {
    if (artifacts == null)
      return null;
    return artifacts.stream().filter(a -> fileName.equals(a.logicalName())).findFirst().orElse(null);
  }

  private String writeLobToSiardStorage(InputStream zis, int fileCount, String extension) throws Exception {
    String outputPath = contentPathExportStrategy.getBlobFilePath(-1, -1, -1, -1) + fileCount + "." + extension;
    OutputStream out = SIARDDKFileIndexFileStrategy.getLOBWriter(baseContainer, outputPath, writeStrategy);
    zis.transferTo(out);
    SIARDDKFileIndexFileStrategy.addFile(outputPath);
    return outputPath;
  }

  private void writeLobReferenceToXml(int columnIndex) throws IOException {
    tableXmlWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex)).append(">")
      .append(Integer.toString(lobsTracker.getLOBsCount())).append("</c").append(String.valueOf(columnIndex))
      .append(">\n");
  }

  private void whiteNilCell(int columnIndex) throws IOException {
    tableXmlWriter.append(TAB).append(TAB).append("<c").append(String.valueOf(columnIndex))
      .append(" xsi:nil=\"true\"/>").append("\n");
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  private void writeColumnElement(int columnIndex, Object value) {
    // TO-DO: implement this
  }

  private String encodeText(String s) {
    s = s.trim();
    s = s.replace("<", "&lt;");
    s = s.replace(">", "&gt;");
    s = s.replace("&", "&amp;");
    s = s.replace("'", "&apos;");
    s = s.replace("\"", "&quot;");
    return s;
  }
}
