/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"name", "simulateTable", "description", "query", "columns", "primaryKey", "foreignKeys"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomViewConfiguration {

  private String name;
  private boolean simulateTable = false;
  private String description;
  private String query;
  private List<CustomColumnConfiguration> columns;
  private PrimaryKeyConfiguration primaryKey;
  private List<ForeignKeyConfiguration> foreignKeys;

  public CustomViewConfiguration() {
    name = Constants.EMPTY;
    columns = new ArrayList<>();
    description = Constants.EMPTY;
    query = Constants.EMPTY;
    foreignKeys = new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * Should the custom view simulate a table in the archive?
   * <p>
   * This will remove the prefix from the name. The view will still be included in
   * the archive with the prefix to document the archive.
   * 
   * @return Boolean
   */
  public boolean isSimulateTable() {
    return simulateTable;
  }

  public void setSimulateTable(boolean simulateTable) {
    this.simulateTable = simulateTable;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public List<CustomColumnConfiguration> getColumns() {
    return columns;
  }

  public void setColumns(List<CustomColumnConfiguration> columns) {
    this.columns = columns;
  }

  public PrimaryKeyConfiguration getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(PrimaryKeyConfiguration primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<ForeignKeyConfiguration> getForeignKeys() {
    return foreignKeys;
  }

  public void setForeignKeys(List<ForeignKeyConfiguration> foreignKeys) {
    this.foreignKeys = foreignKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CustomViewConfiguration that = (CustomViewConfiguration) o;
    return Objects.equals(name, that.name) &&
            Objects.equals(description, that.description) &&
            Objects.equals(query, that.query) &&
      Objects.equals(columns, that.columns) && Objects.equals(primaryKey, that.primaryKey)
      && Objects.equals(foreignKeys, that.foreignKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getDescription(), getQuery());
  }
}
