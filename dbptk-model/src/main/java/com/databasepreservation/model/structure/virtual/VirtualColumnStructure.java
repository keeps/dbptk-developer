/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.structure.virtual;

import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.model.structure.ColumnStructure;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class VirtualColumnStructure extends ColumnStructure {
  public VirtualColumnStructure() {
    super();
  }

  public VirtualColumnStructure(String id, String name, Type type, Boolean nillable, String description,
    String defaultValue, Boolean isAutoIncrement) {
    super(id, name, type, nillable, description, defaultValue, isAutoIncrement);
  }

}
