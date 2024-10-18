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
