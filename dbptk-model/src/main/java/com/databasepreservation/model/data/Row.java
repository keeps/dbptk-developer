/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.data;

import java.util.ArrayList;
import java.util.List;

/**
 * A table data row container.
 *
 * @author Luis Faria
 */
public class Row {
  private long index;

  private List<Cell> cells;

  /**
   * Empty TableStructure data row constructor
   */
  public Row() {
    this.cells = new ArrayList<Cell>();
  }

  /**
   * TableStructure data row constructor
   *
   * @param index
   *          the sequence number of the row in the table, the first row should
   *          have index 1
   * @param cells
   *          the list of cell within this row
   */
  public Row(long index, List<Cell> cells) {
    this.index = index;
    this.cells = cells;
  }

  /**
   * @return the sequence number of the row in the table, the first row will have
   *         index 1
   */
  public long getIndex() {
    return index;
  }

  /**
   * @param index
   *          the sequence number of the row in the table, the first row should
   *          have index 1
   */
  public void setIndex(long index) {
    this.index = index;
  }

  /**
   * @return the list of cell within this row
   */
  public List<Cell> getCells() {
    return cells;
  }

  /**
   * @param cells
   *          the list of cell within this row
   */
  public void setCells(List<Cell> cells) {
    this.cells = cells;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Row [index=");
    builder.append(index);
    builder.append(", cells=");
    builder.append(cells);
    builder.append("]");
    return builder.toString();
  }

}
