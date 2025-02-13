/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
