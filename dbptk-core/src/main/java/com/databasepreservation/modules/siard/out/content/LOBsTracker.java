package com.databasepreservation.modules.siard.out.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

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

  public LOBsTracker() {
    LOBsCount = 0;
    docCollectionCount = 1;
    folderCount = 0;
    currentTable = 0;
    columnIndicesOfLOBsInTables = new HashMap<Integer, List<Integer>>();
    lobTypes = new HashMap<Integer, Map<Integer, String>>();
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
}

// TO-DO: add comment to methods