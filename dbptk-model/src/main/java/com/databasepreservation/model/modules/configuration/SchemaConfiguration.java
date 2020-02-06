package com.databasepreservation.model.modules.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

  private List<CustomViewConfiguration> customViewConfiguration;
  private List<TableConfiguration> tableConfiguration;
  private List<ViewConfiguration> viewConfiguration;

  public SchemaConfiguration() {
    customViewConfiguration = new ArrayList<>();
    tableConfiguration = new ArrayList<>();
    viewConfiguration = new ArrayList<>();
  }

  /*
   * Behaviour Model
   */
  @JsonIgnore
  public boolean isSelectedTable(String tableName) {
    return tableConfiguration.stream().anyMatch(table -> table.getName().equals(tableName));
  }

  @JsonIgnore
  public boolean isSelectedView(String viewName) {
    return viewConfiguration.stream().anyMatch(view -> view.getName().equals(viewName));
  }

  @JsonIgnore
  public boolean isSelectedColumn(String tableName, String columnName) {
    return tableConfiguration.stream().anyMatch(table -> table.getName().equals(tableName)
      && table.getColumns().stream().anyMatch(column -> column.getName().equals(columnName)));
  }

  @JsonIgnore
  public boolean isMaterializedView(String viewName) {
    return viewConfiguration.stream().anyMatch(view -> view.getName().equals(viewName) && view.isMaterialized());
  }

  @JsonIgnore
  public TableConfiguration getTableConfiguration(String tableName) {
    return tableConfiguration.stream().filter(p -> p.getName().equals(tableName)).findFirst().orElse(null);
  }

  @JsonProperty("tables")
  public List<TableConfiguration> getTableConfigurations() {
    return tableConfiguration;
  }

  public void setTableConfigurations(List<TableConfiguration> tableConfiguration) {
    this.tableConfiguration = tableConfiguration;
  }

  @JsonProperty("views")
  public List<ViewConfiguration> getViewConfigurations() {
    return viewConfiguration;
  }

  public void setViewConfigurations(List<ViewConfiguration> viewConfiguration) {
    this.viewConfiguration = viewConfiguration;
  }

  @JsonProperty("custom")
  public List<CustomViewConfiguration> getCustomViewConfigurations() {
    return customViewConfiguration;
  }

  public void setCustomViewConfigurations(List<CustomViewConfiguration> customViewConfiguration) {
    this.customViewConfiguration = customViewConfiguration;
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
