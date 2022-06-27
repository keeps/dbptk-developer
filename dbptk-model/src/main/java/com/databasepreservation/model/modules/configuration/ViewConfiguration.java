/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"name", "materialized", "columns", "where", "orderBy"})
public class ViewConfiguration {

  private String name;
  private boolean materialized;
  private List<ColumnConfiguration> columns;
  private String where;
  private String orderBy;

  public ViewConfiguration() {
    columns = new ArrayList<>();
    where = Constants.EMPTY;
    orderBy = Constants.EMPTY;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public boolean isMaterialized() {
    return materialized;
  }

  public void setMaterialized(boolean materialized) {
    this.materialized = materialized;
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

  @JsonIgnore
  public ColumnConfiguration getColumnConfiguration(String columnName) {
    return getColumns().stream().filter(p -> p.getName().equals(columnName)).findFirst().orElse(null);
  }
}
