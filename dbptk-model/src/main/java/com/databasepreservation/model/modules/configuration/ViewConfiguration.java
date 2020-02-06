package com.databasepreservation.model.modules.configuration;

import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"name", "materialized", "columns", "where"})
public class ViewConfiguration {

  private String name;
  private boolean materialized;
  private List<String> columns;
  private String where;

  public ViewConfiguration() {
    columns = new ArrayList<>();
    where = Constants.EMPTY;
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

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public String getWhere() {
    return where;
  }

  public void setWhere(String where) {
    this.where = where;
  }
}
