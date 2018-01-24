/**
 * 
 */
package com.databasepreservation.modules.dbml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeEnumeration;
import com.databasepreservation.model.structure.type.SimpleTypeInterval;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.utils.JodaUtils;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DBMLImportModule implements DatabaseImportModule {
  /*
   * Most code in this file is taken from this commit https://github.com/keeps/db
   * -preservation-toolkit/tree/c574fde9655e1c44bf36b719dbe0f2eda8687a54 and some
   * of the methods had their signature changed. This file should be reviewed to
   * fix some of the DBML features that were implemented but were ignored due to
   * method signature changes. This is made easier by looking at the linked commit
   * or its parents.
   */

  private static final Logger LOGGER = LoggerFactory.getLogger(DBMLImportModule.class);

  private static final String SCHEMA_VERSION = "0.2";

  private static final String DEFAULT_SCHEMA_NAME = "dbmldb";

  // private static final String ENCODING = "UTF-8";

  private SAXParser saxParser;

  private InputStream dbml;

  private Reporter reporter;

  /**
   * Interface to handle the binary inputstream lookup
   * 
   * @author Luis Faria
   */
  public interface DBMLBinaryLookup {
    /**
     * Lookup a binary inputstream
     * 
     * @param id
     *          the binary id
     * @return the binary inputstream
     * @throws ModuleException
     */
    public InputStream getBinary(String id) throws ModuleException;
  }

  private DBMLBinaryLookup binLookup;

  private Path dbmlFilePath;

  /**
   * DBML import module constructor using the DBML filename
   * 
   * @param dbmlFilePath
   *          path to the DBML file
   * @throws ModuleException
   *           if the files can not be accessed
   */
  public DBMLImportModule(final Path dbmlFilePath) {
    this.dbmlFilePath = dbmlFilePath;
  }

  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule databaseExportModule) throws ModuleException {
    this.binLookup = new DBMLBinaryLookup() {
      @Override
      public InputStream getBinary(final String id) throws ModuleException {
        try {
          return Files.newInputStream(dbmlFilePath.getParent().resolve(id));
        } catch (IOException e) {
          throw new ModuleException("Could not open the specified DBML LOB file", e);
        }
      }
    };

    try {
      dbml = Files.newInputStream(dbmlFilePath);
    } catch (IOException e) {
      throw new ModuleException("Could not open the specified DBML file", e);
    }

    SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
    try {
      saxParser = saxParserFactory.newSAXParser();
    } catch (SAXException e) {
      throw new ModuleException("Error initializing SAX parser", e);
    } catch (ParserConfigurationException e) {
      throw new ModuleException("Error initializing SAX parser", e);
    }

    DBMLSAXHandler dbmlSAXHandler = new DBMLSAXHandler(binLookup, databaseExportModule);
    try {
      databaseExportModule.initDatabase();
      saxParser.parse(dbml, dbmlSAXHandler);
      if (dbmlSAXHandler.getErrors().size() > 0) {
        throw new ModuleException(dbmlSAXHandler.getErrors());
      }
      databaseExportModule.finishDatabase();
    } catch (SAXException e) {
      throw new ModuleException("Error parsing DBML", e);
    } catch (IOException e) {
      throw new ModuleException("Error reading DBML", e);
    }
    return null;
  }

  /**
   * Provide a reporter through which potential conversion problems should be
   * reported. This reporter should be provided only once for the export module
   * instance.
   *
   * @param reporter
   *          The initialized reporter instance.
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  /**
   * The SAX handler for DBML files
   * 
   * @author Luis Faria
   */
  public class DBMLSAXHandler extends DefaultHandler {

    private final Set<String> types = new HashSet<String>();

    private DBMLBinaryLookup binLookup;

    private DatabaseExportModule databaseExportModule;

    private DatabaseStructure structure;

    private Map<String, Throwable> errors;

    // scope variables
    private TableStructure currentTableStructure;

    private ColumnStructure currentColumnStructure;

    private List<Type> currentType;

    private String currentTypeDescription;

    private String currentTypeOriginalName;

    private PrimaryKey currentPKey;

    private String currentTableDataId;

    private Row currentRow;

    private String currentCellId;

    private List<Cell> currentCell;

    private CustomSimpleCell currentSimpleCell;

    private boolean currentCellIsNull;

    private String lastOpenSchema;

    private int currentTableIndex = 0;
    private int currentFKeyIndex = 1;
    private int currentPKeyIndex = 1;
    private int rowsCount = 0;

    /**
     * The DBML Sax handler constructor
     * 
     * @param binLookup
     *          the interface to lookup binary inputstreams referenced by the DBML
     * @param databaseExportModule
     */
    public DBMLSAXHandler(DBMLBinaryLookup binLookup, DatabaseExportModule databaseExportModule) {
      this.binLookup = binLookup;
      this.databaseExportModule = databaseExportModule;
      structure = null;
      errors = new TreeMap<String, Throwable>();

      // scope variables
      currentTableStructure = null;
      currentColumnStructure = null;
      currentType = null;
      currentTypeDescription = null;
      currentTypeOriginalName = null;
      currentPKey = null;
      currentTableDataId = null;
      currentRow = null;
      currentCellId = null;
      currentCell = null;
      currentSimpleCell = null;
      currentCellIsNull = false;

      // types
      types.add("simpleTypeString");
      types.add("simpleTypeNumericExact");
      types.add("simpleTypeNumericApproximate");
      types.add("simpleTypeBoolean");
      types.add("simpleTypeEnumeration");
      types.add("simpleTypeEnumerationOption");
      types.add("simpleTypeDateTime");
      types.add("simpleTypeInterval");
      types.add("simpleTypeBinary");
      types.add("composedTypeArray");
      types.add("composedTypeStructure");
    }

    /**
     * Get all the errors that occured while parsing the DBML file and sending to
     * the database handler
     * 
     * @return A map of errors, where the key is the errors message and the value is
     *         the exception or null if there was no exception
     */
    public Map<String, Throwable> getErrors() {
      return errors;
    }

    public void startDocument() {
      // nothing do to
    }

    public void endDocument() {
      // nothing to do
    }

    public void startElement(String uri, String localName, String qname, Attributes attr) {
      if (qname.equals("db")) {
        if (structure != null) {
          errors.put("unexpected element: <db>", null);
        } else {
          createStructure(attr);
        }
      } else if (qname.equals("structure")) {
        // nothing to do
      } else if (qname.equals("table")) {
        TableStructure table = setSchemaByTableId(attr);
        if (currentTableStructure != null) {
          errors.put("table not closed: " + currentTableStructure.getId(), null);
        }
        // TODO check if current column and type are null
        currentTableStructure = table;
      } else if (qname.equals("columns")) {
        // nothing to do
      } else if (qname.equals("column")) {
        ColumnStructure column = createColumnStructure(attr);
        if (currentTableStructure != null) {
          currentTableStructure.getColumns().add(column);
          if (currentColumnStructure != null) {
            errors.put("column not closed: " + currentColumnStructure.getId(), null);
          }
          currentColumnStructure = column;
        }
      } else if (qname.equals("type")) {
        if (currentType != null) {
          errors.put("type not closed: " + currentType, null);
        }
        currentType = new Vector<Type>();
        currentTypeDescription = attr.getValue("description");
        currentTypeOriginalName = attr.getValue("originalTypeName");
      } else if (types.contains(qname)) {
        Type type = createType(qname, attr);
        if (currentType.size() == 0) {
          type.setDescription(currentTypeDescription);
          type.setOriginalTypeName(currentTypeOriginalName);
        } else if (currentType.size() > 1) {
          Type tailType = currentType.get(currentType.size() - 1);
          if (tailType instanceof ComposedTypeArray) {
            ((ComposedTypeArray) tailType).setElementType(type);
          } else if (tailType instanceof ComposedTypeStructure) {
            // TODO 2017-02-02 bferreira: add support for composed types. Abut
            // getElements() see
            // https://github.com/keeps/db-preservation-toolkit/tree/c574fde9655e1c44bf36b719dbe0f2eda8687a54
            // ((ComposedTypeStructure) tailType).getElements().add(type);
          }
        }
        currentType.add(type);
      } else if (qname.equals("keys")) {
        // nothing to do
      } else if (qname.equals("pkey")) {
        PrimaryKey pkey = new PrimaryKey();
        pkey.setName("pkey" + currentPKeyIndex);
        if (currentTableStructure != null) {
          currentTableStructure.setPrimaryKey(pkey);
          if (currentPKey != null) {
            errors.put("pkey not closed", null);
          }
          currentPKey = pkey;
        } else {
          errors.put("pkey found outside table structure scope", null);
        }
      } else if (qname.equals("field")) {
        String fieldName = attr.getValue("name");
        if (currentPKey != null) {
          currentPKey.getColumnNames().add(fieldName);
        } else {
          errors.put("Field found outside pkey scope: " + fieldName, null);
        }
      } else if (qname.equals("fkey")) {
        ForeignKey fkey = createForeignKey(attr);
        if (currentTableStructure != null) {
          currentTableStructure.getForeignKeys().add(fkey);
        } else {
          errors.put("fkey found outside table structure scope", null);
        }
      } else if (qname.equals("data")) {
        // nothing to do
      } else if (qname.equals("tableData")) {
        String tableDataId = attr.getValue("id");
        LOGGER.debug("importing data of table " + tableDataId);
        if (currentTableDataId != null) {
          errors.put("tableData " + tableDataId + " opened without tableData " + currentTableDataId + " being closed",
            null);
          try {
            LOGGER.debug("handleDataCloseTable: start elem");
            databaseExportModule.handleDataCloseTable(currentTableDataId);
          } catch (ModuleException e) {
            errors.put("Error handling close of table " + currentTableDataId, e);
          }
        }
        try {
          LOGGER.debug("handleDataOpenTable");
          openSchemaForTable(tableDataId);
          databaseExportModule.handleDataOpenTable(tableDataId);
          currentTableDataId = tableDataId;
          rowsCount = 0;
        } catch (ModuleException e) {
          errors.put("Error handling open of table " + tableDataId, e);
        }

      } else if (qname.equals("row")) {
        Row row = new Row();
        row.setIndex(parseInteger(attr.getValue("id")).intValue());
        if (currentRow != null) {
          errors.put("Row not closed, index: " + currentRow.getIndex(), null);
        }
        currentRow = row;
      } else if (qname.equals("cell")) {
        if (currentCell != null || currentCellId != null) {
          errors.put("Cell not closed: " + currentCellId, null);
        }
        currentCellId = attr.getValue("id");
        currentCell = new Vector<Cell>();
      } else if (qname.equals("s")) {
        if (currentCell != null && currentCellId != null) {
          if (currentSimpleCell != null) {
            errors.put("Simple cell not closed: " + currentCellId, null);
          }
          // FIXME 2017-02-02 bferreira: figure out how to use NullCell here
          currentSimpleCell = new CustomSimpleCell(currentCellId, null);
          currentCell.add(currentSimpleCell);
          currentCellIsNull = attr.getValue("xsi:nil") != null && attr.getValue("xsi:nil").equals("true");

        } else {
          errors.put("Simple cell element with no cell id defined" + " in scope", null);
        }
      } else if (qname.equals("c")) {
        if (currentCell != null && currentCellId != null) {
          // FIXME 2017-02-02 bferreira: figure out how to use NullCell here
          currentCell.add(new ComposedCell(currentCellId, null));
        }
      } else if (qname.equals("b")) {
        if (currentCell != null && currentCellId != null) {
          BinaryCell b;
          String fileName = attr.getValue("file");
          // TODO 2017-02-02 bferreira: handle file formats
          String formatRegistryName = attr.getValue("formatRegistryName");
          String formatRegistryKey = attr.getValue("formatRegistryKey");
          try {
            InputStream stream = binLookup.getBinary(fileName);
            b = new BinaryCell(currentCellId, stream);
            currentCell.add(b);
          } catch (ModuleException e) {
            errors.put("Error looking up binary", e);
          }
        } else {
          errors.put("binary cell outside cell context", null);
        }

      } else {
        errors.put("Unrecognized element opened: " + qname, null);
      }
    }

    public void characters(char buf[], int offset, int len) throws SAXException {
      if (currentSimpleCell != null) {
        String append = currentSimpleCell.getSimpleData();
        if (append == null) {
          append = "";
        }
        // currentSimpleCell.setSimpleData(append
        // + unencode(new String(buf, offset, len)));
        currentSimpleCell.setSimpleData(append + new String(buf, offset, len));
      }
    }

    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
      if (currentSimpleCell != null) {
        LOGGER.warn("found ignorable whitespace inside a simple cell: '" + new String(ch, start, length) + "'");
      }
    }

    public void endElement(String uri, String localName, String qname) throws SAXException {
      if (qname.equals("db")) {
        // nothing to do
      } else if (qname.equals("structure")) {
        try {
          databaseExportModule.handleStructure(structure);
        } catch (ModuleException e) {
          errors.put("Error handling structure", e);

        }
      } else if (qname.equals("table")) {
        currentTableStructure = null;
      } else if (qname.equals("columns")) {
        // nothing to do
      } else if (qname.equals("column")) {
        currentColumnStructure = null;
      } else if (qname.equals("type")) {
        currentType = null;
        currentTypeDescription = null;
        currentTypeOriginalName = null;
      } else if (types.contains(qname)) {
        if (currentType != null) {
          if (currentType.size() == 1 && currentColumnStructure != null) {
            currentColumnStructure.setType(currentType.get(0));
          }
          currentType.remove(currentType.size() - 1);
        } else {
          errors.put("Type closed with no type opened in scope", null);
        }
      } else if (qname.equals("keys")) {
        // nothing to do
      } else if (qname.equals("pkey")) {
        currentPKey = null;
      } else if (qname.equals("field")) {
        // nothing to do
      } else if (qname.equals("fkey")) {
        // nothing to do
      } else if (qname.equals("data")) {
        try {
          databaseExportModule.handleDataCloseSchema(lastOpenSchema);
        } catch (ModuleException e) {
          errors.put("error closing schema", e);
        }
      } else if (qname.equals("tableData")) {
        try {
          structure.getTableById(currentTableDataId).setRows(rowsCount);
          databaseExportModule.handleDataCloseTable(currentTableDataId);
        } catch (ModuleException e) {
          errors.put("Error closing table", null);
        }
        currentTableDataId = null;
      } else if (qname.equals("row")) {
        if (currentRow != null) {
          try {
            rowsCount++;
            databaseExportModule.handleDataRow(currentRow);
          } catch (ModuleException e) {
            errors.put("Error handling row " + currentRow.getIndex() + " of table " + currentTableDataId, e);
          }
          currentRow = null;
        } else {
          errors.put("row closed without being opened", null);
        }

      } else if (qname.equals("cell")) {
        if (currentCell != null && currentCell.size() == 1) {
          if (currentRow != null) {
            currentRow.getCells().add(currentCell.get(0));
          } else {
            errors.put("Cell outside row context", null);
          }
        }
        currentCellId = null;
        currentCell = null;
      } else if (qname.equals("s")) {
        if (currentCell != null) {
          if (currentSimpleCell.getSimpleData() == null && !currentCellIsNull) {
            currentSimpleCell.setSimpleData("");
          }
          if (currentCell.size() == 1) {
            if (currentCell.get(0) == currentSimpleCell) {
              currentSimpleCell = null;
            } else {
              errors.put("current cell inconsistent", null);
            }
          } else if (currentCell.size() > 1) {
            Cell parentCell = currentCell.get(currentCell.size() - 2);
            if (parentCell instanceof ComposedCell) {
              ((ComposedCell) parentCell).getComposedData().add(currentSimpleCell);
            } else {
              errors.put("Non composed cell with subcells: " + parentCell.getId(), null);
            }
          } else {
            errors.put("simple cell outside cell context", null);
          }
        } else {
          errors.put("simple cell outside cell context", null);
        }
      } else if (qname.equals("c")) {
        if (currentCell.size() == 1) {
          // do nothing
        } else if (currentCell.size() > 1) {
          Cell parentCell = currentCell.get(currentCell.size() - 2);
          Cell childCell = currentCell.remove(currentCell.size() - 1);
          if (parentCell instanceof ComposedCell) {
            ((ComposedCell) parentCell).getComposedData().add(childCell);
          } else {
            errors.put("Non composed cell with subcells: " + parentCell.getId(), null);
          }
        } else {
          errors.put("Composed cell outside cell context", null);
        }
      } else if (qname.equals("b")) {
        if (currentCell.size() == 1) {
          // do nothing
        } else if (currentCell.size() > 1) {
          Cell parentCell = currentCell.get(currentCell.size() - 2);
          Cell childCell = currentCell.remove(currentCell.size() - 1);
          if (parentCell instanceof ComposedCell) {
            ((ComposedCell) parentCell).getComposedData().add(childCell);
          } else {
            errors.put("Non composed cell with subcells: " + parentCell.getId(), null);
          }
        } else {
          errors.put("Binary cell outside cell context", null);
        }
      } else {
        errors.put("Unrecognized element closed: " + qname, null);
      }
    }

    private void createStructure(Attributes attr) {
      structure = new DatabaseStructure();
      structure.setUsers(Arrays.asList(new UserStructure("root", null)));
      structure.setName(attr.getValue("name"));
      structure.setProductName(attr.getValue("productName"));
      structure.setProductVersion(attr.getValue("productVersion"));
      structure.setDefaultTransactionIsolationLevel(parseInteger(attr.getValue("defaultTransactionIsolationLevel")));
      structure.setExtraNameCharacters(attr.getValue("extraNameCharacters"));
      structure.setStringFunctions(attr.getValue("stringFunctions"));
      structure.setSystemFunctions(attr.getValue("systemFunctions"));
      structure.setTimeDateFunctions(attr.getValue("timeDateFunctions"));
      structure.setUrl(attr.getValue("url"));
      structure.setSupportsANSI92EntryLevelSQL(parseBoolean(attr.getValue("supportsANSI92EntryLevelSQL")));
      structure.setSupportsANSI92IntermediateSQL(parseBoolean(attr.getValue("supportsANSI92IntermediateSQL")));
      structure.setSupportsANSI92FullSQL(parseBoolean(attr.getValue("supportsANSI92FullSQL")));
      structure.setSupportsCoreSQLGrammar(parseBoolean(attr.getValue("supportsCoreSQLGrammar")));

      String datetime = attr.getValue("creationDate");
      if (StringUtils.isNotBlank(datetime)) {
        structure.setArchivalDate(JodaUtils.xsDatetimeParse(datetime));
      }

      if (!attr.getValue("schemaVersion").equals(SCHEMA_VERSION)) {
        errors.put(
          "Schema version is different from the supported " + attr.getValue("schemaVersion") + "!=" + SCHEMA_VERSION,
          null);
      }
    }

    private TableStructure setSchemaByTableId(Attributes attr) {
      String[] parts = attr.getValue("id").split("\\.");
      if (parts.length < 2) {
        LOGGER.warn("Using {} as schema name", DEFAULT_SCHEMA_NAME);
      }
      SchemaStructure schema = structure.getSchemaByName(parts[0]);
      if (schema == null) {
        schema = new SchemaStructure();
        schema.setName(parts[0]);
        structure.getSchemas().add(schema);
        currentTableIndex = 0;
      }
      TableStructure table = createTableStructure(attr);
      table.setSchema(schema);
      schema.getTables().add(table);

      return table;
    }

    private TableStructure createTableStructure(Attributes attr) {
      TableStructure table = new TableStructure();
      table.setId(attr.getValue("id"));
      table.setName(attr.getValue("name"));
      table.setDescription(attr.getValue("description"));
      table.setIndex(currentTableIndex);
      currentTableIndex++;
      return table;
    }

    private ColumnStructure createColumnStructure(Attributes attr) {
      ColumnStructure column = new ColumnStructure();
      column.setId(attr.getValue("id"));
      column.setName(attr.getValue("name"));
      column.setNillable(parseBoolean(attr.getValue("nillable")));
      column.setDescription(attr.getValue("description"));
      return column;
    }

    private Type createType(String qname, Attributes attr) {
      Type type = null;
      if (qname.equals("simpleTypeString")) {
        type = new SimpleTypeString(parseInteger(attr.getValue("length")), parseBoolean(attr.getValue("variableLegth")),
          attr.getValue("charSet"));
      } else if (qname.equals("simpleTypeNumericExact")) {
        type = new SimpleTypeNumericExact(parseInteger(attr.getValue("precision")),
          parseInteger(attr.getValue("scale")));
      } else if (qname.equals("simpleTypeNumericApproximate")) {
        type = new SimpleTypeNumericApproximate(parseInteger(attr.getValue("precision")));
      } else if (qname.equals("simpleTypeBoolean")) {
        type = new SimpleTypeBoolean();
      } else if (qname.equals("simpleTypeEnumeration")) {
        type = new SimpleTypeEnumeration();
      } else if (qname.equals("simpleTypeEnumerationOption")) {
        String optionName = attr.getValue("name");
        if (currentType.size() > 0 && currentType.get(currentType.size() - 1) instanceof SimpleTypeEnumeration) {
          ((SimpleTypeEnumeration) currentType.get(currentType.size() - 1)).getOptions().add(optionName);
        } else {
          errors.put("Enumeration option element must be inside an" + " enumeration element", null);
        }
      } else if (qname.equals("simpleTypeDateTime")) {
        type = new SimpleTypeDateTime(parseBoolean(attr.getValue("timeDefined")),
          parseBoolean(attr.getValue("timeZoneDefined")));
      } else if (qname.equals("simpleTypeInterval")) {
        String intervalType = attr.getValue("type");
        if (intervalType.equals("START_END")) {
          type = new SimpleTypeInterval(SimpleTypeInterval.IntervalType.STARTDATE_ENDDATE);
        } else if (intervalType.equals("START_DURATION")) {
          type = new SimpleTypeInterval(SimpleTypeInterval.IntervalType.STARTDATE_DURATION);
        } else if (intervalType.equals("DURATION_END")) {
          type = new SimpleTypeInterval(SimpleTypeInterval.IntervalType.DURATION_ENDDATE);
        } else if (intervalType.equals("DURATION")) {
          type = new SimpleTypeInterval(SimpleTypeInterval.IntervalType.DURATION);
        } else {
          errors.put("Wrong interval type value: " + intervalType, null);
        }

      } else if (qname.equals("simpleTypeBinary")) {
        type = new SimpleTypeBinary(attr.getValue("formatRegistryName"), attr.getValue("formatRegistryKey"));
      } else if (qname.equals("composedTypeArray")) {
        type = new ComposedTypeArray();
      } else if (qname.equals("composedTypeStructure")) {
        type = new ComposedTypeStructure();
      } else {
        errors.put("Unrecognized type: " + qname, null);
      }

      try {
        type.setSql99TypeName(convertDBMLTypeToSql99Type(type));
      } catch (UnknownTypeException e) {
        errors.put("Could not define the SQL99TypeName for " + type.getOriginalTypeName(), null);
      }
      return type;
    }

    private String convertDBMLTypeToSql99Type(Type type) throws UnknownTypeException {
      String ret = null;

      if (type instanceof SimpleTypeString) {
        SimpleTypeString string = (SimpleTypeString) type;
        if (string.isLengthVariable()) {
          ret = "CHARACTER VARYING(" + ((SimpleTypeString) type).getLength() + ")";
        } else {
          ret = "CHARACTER";
        }
      } else if (type instanceof SimpleTypeNumericExact) {
        ret = "DECIMAL";
      } else if (type instanceof SimpleTypeNumericApproximate) {
        ret = "FLOAT";
      } else if (type instanceof SimpleTypeBoolean) {
        ret = "BOOLEAN";
      } else if (type instanceof SimpleTypeDateTime) {
        SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
        if (dateTime.getTimeDefined()) {
          ret = "TIMESTAMP";
        } else {
          ret = "DATE";
        }
      } else if (type instanceof SimpleTypeBinary) {
        ret = "BIT VARYING";
      } else {
        throw new UnknownTypeException(type.toString());
      }
      return ret;
    }

    private ForeignKey createForeignKey(Attributes attr) {
      ForeignKey foreignKey = new ForeignKey();
      String[] schema_table = attr.getValue("in").split("\\.");
      foreignKey.setId(attr.getValue("id"));
      foreignKey.setName("fkey" + currentFKeyIndex);
      currentFKeyIndex++;
      foreignKey.setReferencedSchema(schema_table[0]);
      foreignKey.setReferencedTable(schema_table[1]);
      foreignKey.setReferences(Arrays.asList(new Reference(attr.getValue("name"), attr.getValue("ref"))));
      return foreignKey;
    }

    public void warning(SAXParseException spe) {
      LOGGER.warn("Warning caught while parsing", spe);
    }

    public void fatalError(SAXParseException e) throws SAXException {
      LOGGER.error("Fatal error caught while parsing", e);
      throw e;
    }

    private Integer parseInteger(String value) {
      return value != null ? Integer.valueOf(value) : null;
    }

    private Boolean parseBoolean(String value) {
      return value != null ? Boolean.valueOf(value) : null;
    }

    private void openSchemaForTable(String tableId) {
      String[] split = tableId.split("\\.");

      if (lastOpenSchema == null) {
        try {
          databaseExportModule.handleDataOpenSchema(split[0]);
          lastOpenSchema = split[0];
        } catch (ModuleException e) {
          errors.put("could not open schema " + split[0], e);
        }
      } else if (!lastOpenSchema.equals(split[0])) {
        try {
          databaseExportModule.handleDataCloseSchema(lastOpenSchema);
        } catch (ModuleException e) {
          errors.put("could not close schema " + split[0], e);
        }

        try {
          databaseExportModule.handleDataOpenSchema(split[0]);
          lastOpenSchema = split[0];
        } catch (ModuleException e) {
          errors.put("could not open schema " + split[0], e);
        }
      }
    }
  }

  /**
   * The (old) DBML module was changing the value of the SimpleCell, which is no
   * longer allowed, so a new class was created to allow it until this module can
   * be refactored.
   */
  private static class CustomSimpleCell extends SimpleCell {
    private String simpleData;

    protected CustomSimpleCell(String id) {
      super(id);
      simpleData = null;
    }

    public CustomSimpleCell(String id, String simpleData) {
      super(id, simpleData);
      this.simpleData = simpleData;
    }

    @Override
    public String getSimpleData() {
      return simpleData;
    }

    void setSimpleData(String simpleData) {
      this.simpleData = simpleData;
    }
  }
}
