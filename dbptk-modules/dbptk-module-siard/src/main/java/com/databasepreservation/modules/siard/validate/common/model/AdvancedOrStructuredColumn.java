/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common.model;

import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class AdvancedOrStructuredColumn {
  private String name;
  private String typeSchema;
  private String typeName;
  private List<Field> fields;

  public AdvancedOrStructuredColumn(String name, String typeSchema, String typeName, List<Field> fields) {
    this.name = name;
    this.typeSchema = typeSchema;
    this.typeName = typeName;
    this.fields = fields;
  }

  public String getName() {
    return name;
  }

  public String getTypeSchema() {
    return typeSchema;
  }

  public String getTypeName() {
    return typeName;
  }

  public List<Field> getFields() {
    return fields;
  }

  public String getType() {
    return this.typeSchema + "." + this.typeName;
  }
}
