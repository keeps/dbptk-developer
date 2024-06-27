package com.databasepreservation.model.structure.virtual;

import com.databasepreservation.model.structure.PrimaryKey;

import java.util.List;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class VirtualPrimaryKey extends PrimaryKey {

  public VirtualPrimaryKey() {
    super();
  }

  public VirtualPrimaryKey(String name, List<String> columns, String description) {
    super(name, columns, description);
  }
}
