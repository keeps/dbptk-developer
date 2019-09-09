/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class Field {
  private String name;
  private List<Field> fields;

  public Field() {
    this.fields = new ArrayList<>();
  }

  public Field(String name, List<Field> fields) {
    this.name = name;
    this.fields = fields;
  }

  public String getName() {
    return name;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public void addFieldToList(Field field) {
    this.fields.add(field);
  }
}
