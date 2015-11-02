package com.databasepreservation.modules.siard.out.content;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.stream.StreamSource;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.input.sax.XMLReaderJDOMFactory;
import org.jdom2.input.sax.XMLReaderXSDFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class LOBsTracker {

  // private int tableCount;
  private int LOBsCount; // Total LOBsCount in archive
  private int docCollectionCount;
  private int folderCount; // Folder within docCollection
  private int currentTable;
  private Map<Integer, List<Integer>> columnIndicesOfLOBsInTables; // Info like:
                                                                   // table 7
                                                                   // has LOBs
                                                                   // in columns
                                                                   // 3, 4 and 7
  private List<Integer> lobColumnsIndices;
  private Map<Integer, Map<Integer, String>> lobTypes; // Info like: table 7,
                                                       // column 2 is a "BLOB"
  private Map<Integer, String> lobTypeInColumn;
  private Map<String, String> docIDs;

  private SIARDDKExportModule siarddkExportModule;

  public LOBsTracker(SIARDDKExportModule siarddkExportModule) {
    LOBsCount = 0;
    docCollectionCount = 1;
    folderCount = 0;
    currentTable = 0;
    columnIndicesOfLOBsInTables = new HashMap<Integer, List<Integer>>();
    lobTypes = new HashMap<Integer, Map<Integer, String>>();
    docIDs = new HashMap<String, String>();
    this.siarddkExportModule = siarddkExportModule;
  }

  /**
   * 
   * @return The total number of LOBs in the archive
   */
  public int getLOBsCount() {
    return LOBsCount;
  }

  /**
   * @return the docCollectionCount
   */
  public int getDocCollectionCount() {
    return docCollectionCount;
  }

  /**
   * @return the folderCount
   */
  public int getFolderCount() {
    return folderCount;
  }

  public String getLOBsType(int table, int column) {
    if (lobTypes.get(table) != null) {
      return lobTypes.get(table).get(column);
    }
    return null;
  }

  public void addLOBLocationAndType(int table, int column, String typeOfLOB) {

    if (table > currentTable) {
      lobColumnsIndices = new ArrayList<Integer>();
      lobTypeInColumn = new HashMap<Integer, String>();
      currentTable = table;
    }

    if (!columnIndicesOfLOBsInTables.containsKey(table)) {
      columnIndicesOfLOBsInTables.put(table, lobColumnsIndices);
      lobTypes.put(table, lobTypeInColumn);
    }

    if (!lobColumnsIndices.contains(column)) {
      lobColumnsIndices.add(column);
      lobTypeInColumn.put(column, typeOfLOB);
    }
  }

  public void addLOB() {
    LOBsCount += 1;
    folderCount += 1;

    // Note: code assumes one file in each folder

    if (folderCount == SIARDDKConstants.MAX_NUMBER_OF_FILES + 1) {
      docCollectionCount += 1;
      folderCount = 1;
    }
  }

  public void addDocID(String tableName, String columnName) throws ModuleException {

    if (isDocID(tableName, columnName)) {
      throw new ModuleException("Same documentIdebtification added twice");
    }

    docIDs.put(tableName, columnName);
  }

  public boolean isDocID(String tableName, String columnName) {
    if (docIDs.get(tableName) != null) {
      return docIDs.get(tableName).equals(columnName);
    }
    return false;
  }

  public void getDocIDsFromCommandLine() throws ModuleException {

    MetadataPathStrategy metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    Map<String, String> exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    String pathStr = exportModuleArgs.get(SIARDDKConstants.DOCUMENT_IDENTIFICATION);

    InputStream in = this.getClass().getResourceAsStream(
      metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.DOCUMENT_IDENTIFICATION));
    XMLReaderJDOMFactory schemaFactory;
    try {
      schemaFactory = new XMLReaderXSDFactory(new StreamSource(in));
    } catch (JDOMException e) {
      throw new ModuleException("Problem creating JDOM schema factory", e);
    }

    SAXBuilder builder = new SAXBuilder(schemaFactory);

    try {
      in.close();
    } catch (IOException e) {
      throw new ModuleException("Could not close resource inputstream", e);
    }

    // Read index xml-file given on command line and validate against schema

    File indexXmlFile = new File(pathStr);
    try {
      Document document = builder.build(indexXmlFile);

      // Namespace ns = Namespace.getNamespace(SIARDDKConstants.DBPTK_NS);

      Element rootElement = document.getRootElement();
      List<Element> docIDList = rootElement.getChildren();
      String tableName;
      String columnName;
      for (Element docID : docIDList) {
        tableName = docID.getAttributeValue("tableName");
        columnName = docID.getAttributeValue("columnName");
        addDocID(tableName, columnName);
      }
    } catch (JDOMException e) {
      throw new ModuleException("There was a problem building the JDOM document for " + pathStr, e);
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}

// TO-DO: add comment to methods