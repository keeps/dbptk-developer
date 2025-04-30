package com.databasepreservation.model.modules.configuration;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class ForeignKeyConfiguration {

  private String name;
  private String referencedSchema;
  private String referencedTable;
  private List<ReferenceConfiguration> references;
  private String description;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getReferencedSchema() {
    return referencedSchema;
  }

  public void setReferencedSchema(String referencedSchema) {
    this.referencedSchema = referencedSchema;
  }

  public String getReferencedTable() {
    return referencedTable;
  }

  public void setReferencedTable(String referencedTable) {
    this.referencedTable = referencedTable;
  }

  public List<ReferenceConfiguration> getReferences() {
    return references;
  }

  public void setReferences(List<ReferenceConfiguration> references) {
    this.references = references;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass())
      return false;
    ForeignKeyConfiguration that = (ForeignKeyConfiguration) o;
    return Objects.equals(name, that.name) && Objects.equals(referencedSchema, that.referencedSchema)
      && Objects.equals(referencedTable, that.referencedTable) && Objects.equals(references, that.references)
      && Objects.equals(description, that.description);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, referencedSchema, referencedTable, references, description);
  }
}
