/**
 *
 */
package com.databasepreservation.model.data;

import java.util.List;

/**
 * Container for composed data
 *
 * @author Luis Faria
 */
public class ComposedCell extends Cell {

  private List<Cell> composedData;

  /**
   * Create a composed cell without data. This should not be used to represent
   * NULL, instead a NullCell should be created.
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   */
  protected ComposedCell(String id) {
    super(id);
  }

  /**
   * Create a composed cell.
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   * @param composedData
   *          the list of cells that compose this cell
   */
  public ComposedCell(String id, List<Cell> composedData) {
    super(id);
    this.composedData = composedData;
  }

  /**
   * @return the list of cells that compose this cell
   */
  public List<Cell> getComposedData() {
    return composedData;
  }

  /**
   * @param composedData
   *          the list of cells that compose this cell
   */
  public void setComposedData(List<Cell> composedData) {
    this.composedData = composedData;
  }

}
