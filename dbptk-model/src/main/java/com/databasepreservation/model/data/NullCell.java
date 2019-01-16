/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
