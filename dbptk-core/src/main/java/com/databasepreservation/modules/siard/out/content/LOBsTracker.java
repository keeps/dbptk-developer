package com.databasepreservation.modules.siard.out.content;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class LOBsTracker {

  private int LOBsCount; // Total LOBsCount in archive
  private int docCollectionCount;
  private int folderCount; // Folder within docCollection
  private int currentTable;
  private int maxLobsPerFolder;
  private int maxLobsFolderSize;
  private int accumulatedLobsSize;
  private Map<Integer, List<Integer>> columnIndicesOfLOBsInTables; // Info like:
                                                                   // table 7
                                                                   // has LOBs
                                                                   // in columns
                                                                   // 3, 4 and 7
  private List<Integer> lobColumnsIndices;
  private Map<Integer, Map<Integer, String>> lobTypes; // Info like: table 7,
                                                       // column 2 is a "BLOB"
  private Map<Integer, String> lobTypeInColumn;
  private Map<Integer, Map<Integer, Integer>> maxCLOBlength;

  public LOBsTracker(int maxLobsPerFolder, int maxLobsFolderSize) {
    LOBsCount = 0;
    docCollectionCount = 1;
    folderCount = 0;
    currentTable = 0;
    this.maxLobsPerFolder = maxLobsPerFolder;
    this.maxLobsFolderSize = maxLobsFolderSize;
    accumulatedLobsSize = 0;
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
   * @return the accumulatedLobsSize
   */
  public int getAccumulatedLobsSize() {
    return accumulatedLobsSize;
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

  /**
   * Adds LOB of given size in megabyte
   * 
   * @param lobSize
   *          The size of the LOB in megabyte
   */
  public void addLOB(int lobSize) {
    LOBsCount += 1;
    folderCount += 1;

    // Note: code assumes one file in each folder

    int newPotentialAccumulatedSize = accumulatedLobsSize + lobSize;
    if (newPotentialAccumulatedSize > maxLobsFolderSize || folderCount == maxLobsPerFolder + 1) {
      docCollectionCount += 1;
      folderCount = 1;
      accumulatedLobsSize = lobSize;
    } else {
      accumulatedLobsSize += lobSize;
    }
  }

  public void updateMaxClobLength(int table, int column, int length) {
    if (maxCLOBlength == null) {
      maxCLOBlength = new HashMap<Integer, Map<Integer, Integer>>();
    }

    Map<Integer, Integer> maxClobLengthInColumn = maxCLOBlength.get(table);
    if (maxClobLengthInColumn == null) {
      maxClobLengthInColumn = new HashMap<Integer, Integer>();
      maxCLOBlength.put(table, maxClobLengthInColumn);
    }

    if (!maxClobLengthInColumn.containsKey(column)) {
      maxClobLengthInColumn.put(column, length);
    } else {
      if (maxClobLengthInColumn.get(column) < length) {
        maxClobLengthInColumn.put(column, length);
      }
    }
  }

  /**
   * 
   * @param table
   * @param column
   * @return The maximum CLOB length set so far. Returns -1 if not set
   */
  public int getMaxClobLength(int table, int column) {
    if (maxCLOBlength == null) {
      return -1;
    }

    Map<Integer, Integer> maxClobLengthInColumn = maxCLOBlength.get(table);
    if (maxClobLengthInColumn == null) {
      return -1;
    }

    if (maxClobLengthInColumn.containsKey(column)) {
      return maxClobLengthInColumn.get(column);
    } else {
      return -1;
    }
  }

  /**
   * Must only be called after addLOB method has be called
   */
  public void decrementLOBsCount() {
    LOBsCount -= 1;

    if (folderCount == 1) {
      folderCount = maxLobsPerFolder;
      docCollectionCount -= 1;
    } else {
      folderCount -= 1;
    }
  }
}

// TO-DO: add comment to methods