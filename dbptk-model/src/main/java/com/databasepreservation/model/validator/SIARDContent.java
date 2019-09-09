/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.validator;

import java.util.Objects;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDContent implements Comparable<SIARDContent> {

  private String schema;
  private String table;

  public SIARDContent(String schema, String table) {
    this.schema = schema;
    this.table = table;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    SIARDContent that = (SIARDContent) o;
    return Objects.equals(getSchema(), that.getSchema()) && Objects.equals(getTable(), that.getTable());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSchema(), getTable());
  }

  @Override
  public int compareTo(SIARDContent anotherContentPath) {
    int number = Integer.parseInt(schema.replace("schema", ""));

    final String anotherSchema = anotherContentPath.getSchema();
    int anotherNumber = Integer.parseInt(anotherSchema.replace("schema", ""));

    int res = number - anotherNumber;

    number = Integer.parseInt(table.replace("table", ""));

    final String anotherTable = anotherContentPath.getTable();
    anotherNumber = Integer.parseInt(anotherTable.replace("table", ""));

    if (res != 0) {
      return res;
    } else {
      return number - anotherNumber;
    }
  }
}
