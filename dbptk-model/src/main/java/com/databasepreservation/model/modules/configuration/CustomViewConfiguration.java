/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"name", "description", "query"})
public class CustomViewConfiguration {

  private String name;
  private String description;
  private String query;
  private List<ColumnConfiguration> columns;

  public CustomViewConfiguration() {
    name = Constants.EMPTY;
    columns = new ArrayList<>();
    description = Constants.EMPTY;
    query = Constants.EMPTY;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  public List<ColumnConfiguration> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnConfiguration> columns) {
    this.columns = columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CustomViewConfiguration that = (CustomViewConfiguration) o;
    return Objects.equals(name, that.name) &&
            Objects.equals(description, that.description) &&
            Objects.equals(query, that.query) &&
            Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getDescription(), getQuery());
  }
}
