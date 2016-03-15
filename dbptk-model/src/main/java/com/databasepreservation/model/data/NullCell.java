package com.databasepreservation.model.data;

/**
 * Cell representing null value
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class NullCell extends Cell {
  /**
   * Abstract Cell constructor
   *
   * @param id
   *          the cell id, equal to 'tableId.columnId.rowIndex'
   */
  public NullCell(String id) {
    super(id);
  }
}
