package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "description", "nillable", "merkle", "inventory", "externalLOB"})
public class CustomColumnConfiguration extends ColumnConfiguration {
  private Boolean nillable;
  private String description;

  public Boolean getNillable() {
    return nillable;
  }

  public void setNillable(Boolean nillable) {
    this.nillable = nillable;
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
    if (!super.equals(o))
      return false;
    CustomColumnConfiguration that = (CustomColumnConfiguration) o;
    return Objects.equals(getNillable(), that.getNillable()) && Objects.equals(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getNillable(), getDescription());
  }
}
