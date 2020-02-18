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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"name", "columns", "where", "orderBy"})
public class TableConfiguration {

  private String name;
  private List<ColumnConfiguration> columns;
  private String where;
  private String orderBy;

  public TableConfiguration() {
    columns = new ArrayList<>();
    where = Constants.EMPTY;
    orderBy = Constants.EMPTY;
  }

  @JsonIgnore
  public ColumnConfiguration getColumnConfiguration(String columnName) {
    return getColumns().stream().filter(p -> p.getName().equals(columnName)).findFirst().orElse(null);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<ColumnConfiguration> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnConfiguration> columns) {
    this.columns = columns;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }

  public String getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(String orderBy) {
    this.orderBy = orderBy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    TableConfiguration that = (TableConfiguration) o;
    return Objects.equals(getName(), that.getName()) && Objects.equals(getColumns(), that.getColumns())
      && Objects.equals(getWhere(), that.getWhere()) && Objects.equals(getOrderBy(), that.getOrderBy());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getColumns(), getWhere(), getOrderBy());
  }
}
