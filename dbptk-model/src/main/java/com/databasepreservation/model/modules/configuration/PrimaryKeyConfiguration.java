package com.databasepreservation.model.modules.configuration;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PrimaryKeyConfiguration {

  private String name;
  private List<String> columnNames;
  private String description;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PrimaryKeyConfiguration that = (PrimaryKeyConfiguration) o;
    return Objects.equals(name, that.name) && Objects.equals(columnNames, that.columnNames)
      && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, columnNames, description);
  }
}
