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
  private Map<Integer, List<Integer>> LOBsLocations;
  private List<Integer> LOBsColumns;

  public LOBsTracker() {
    // tableCount = 0;
    LOBsCount = 1;
    docCollectionCount = 1;
    folderCount = 1;
    currentTable = 0;
    LOBsLocations = new HashMap<Integer, List<Integer>>();
  }

  // public int getTableCount() {
  // return tableCount;
  // }

  // public void incrementTableCount() {
  // tableCount += 1;
  // }

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

  public void addLOB(int table, int column) {

    if (table > currentTable) {
      LOBsColumns = new ArrayList<Integer>();
      currentTable = table;
    }

    if (!LOBsLocations.containsKey(table)) {
      LOBsLocations.put(table, LOBsColumns);
    }

    if (!LOBsColumns.contains(column)) {
      LOBsColumns.add(column);
    }

    LOBsCount += 1;
    folderCount += 1;

    // Note: code assumes one file in each folder

    if (folderCount == SIARDDKConstants.MAX_NUMBER_OF_FILES) {
      docCollectionCount += 1;
      folderCount = 1;
    }
  }
}

// TO-DO: add comment to methods