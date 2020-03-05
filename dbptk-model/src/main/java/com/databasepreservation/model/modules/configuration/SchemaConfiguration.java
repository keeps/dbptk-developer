/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({"custom", "tables", "views"})
public class SchemaConfiguration {

  private List<CustomViewConfiguration> customViewConfigurations;
  private List<TableConfiguration> tableConfigurations;
  private List<ViewConfiguration> viewConfigurations;

  public SchemaConfiguration() {
    customViewConfigurations = new ArrayList<>();
    tableConfigurations = new ArrayList<>();
    viewConfigurations = new ArrayList<>();
  }

  /*
   * Behaviour Model
   */
  @JsonIgnore
  public boolean isSelectedTable(String tableName) {
    return tableConfigurations.stream().anyMatch(table -> table.getName().equals(tableName));
  }

  @JsonIgnore
  public boolean isSelectedView(String viewName) {
    return viewConfigurations.stream().anyMatch(view -> view.getName().equals(viewName));
  }

  @JsonIgnore
  public boolean isSelectedColumnFromTable(String tableName, String columnName) {
    return tableConfigurations.stream().anyMatch(table -> table.getName().equals(tableName)
      && table.getColumns().stream().anyMatch(column -> column.getName().equals(columnName)));
  }

  @JsonIgnore
  public boolean isSelectedColumnFromView(String viewName, String columnName) {
    return viewConfigurations.stream().anyMatch(view -> view.getName().equals(viewName)
      && view.getColumns().stream().anyMatch(column -> column.getName().equals(columnName)));
  }

  @JsonIgnore
  public boolean isMerkleColumn(String tableName, String columnName) {
    final boolean viewMerkleColumn = viewConfigurations.stream().anyMatch(
      view -> view.getName().equals(tableName.replace(Constants.VIEW_NAME_PREFIX, "")) && view.isMaterialized()
        && view.getColumns().stream().anyMatch(column -> column.getName().equals(columnName) && column.isMerkle()));

    return viewMerkleColumn || tableConfigurations.stream().anyMatch(table -> table.getName().equals(tableName)
      && table.getColumns().stream().anyMatch(column -> column.getName().equals(columnName) && column.isMerkle()));
  }

  @JsonIgnore
  public boolean isMaterializedView(String viewName) {
    return viewConfigurations.stream().anyMatch(view -> view.getName().equals(viewName) && view.isMaterialized());
  }

  @JsonIgnore
  public TableConfiguration getTableConfiguration(String tableName) {
    return tableConfigurations.stream().filter(p -> p.getName().equals(tableName)).findFirst().orElse(null);
  }

  @JsonIgnore
  public ViewConfiguration getViewConfiguration(String viewName) {
    return viewConfigurations.stream().filter(p -> p.getName().equals(viewName)).findFirst().orElse(null);
  }

  @JsonProperty("tables")
  public List<TableConfiguration> getTableConfigurations() {
    return tableConfigurations;
  }

  public void setTableConfigurations(List<TableConfiguration> tableConfiguration) {
    this.tableConfigurations = tableConfiguration;
  }

  @JsonProperty("views")
  public List<ViewConfiguration> getViewConfigurations() {
    return viewConfigurations;
  }

  public void setViewConfigurations(List<ViewConfiguration> viewConfiguration) {
    this.viewConfigurations = viewConfiguration;
  }

  @JsonProperty("custom")
  public List<CustomViewConfiguration> getCustomViewConfigurations() {
    return customViewConfigurations;
  }

  public void setCustomViewConfigurations(List<CustomViewConfiguration> customViewConfiguration) {
    this.customViewConfigurations = customViewConfiguration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    SchemaConfiguration that = (SchemaConfiguration) o;
    return Objects.equals(getCustomViewConfigurations(), that.getCustomViewConfigurations())
      && Objects.equals(getTableConfigurations(), that.getTableConfigurations())
      && Objects.equals(getViewConfigurations(), that.getViewConfigurations());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCustomViewConfigurations(), getTableConfigurations(), getViewConfigurations());
  }
}
