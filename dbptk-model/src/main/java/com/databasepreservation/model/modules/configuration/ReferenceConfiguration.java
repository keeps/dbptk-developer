package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

public class ReferenceConfiguration {

  private String column;
  private String referenced;

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getReferenced() {
    return referenced;
  }

  public void setReferenced(String referenced) {
    this.referenced = referenced;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ReferenceConfiguration that = (ReferenceConfiguration) o;
    return Objects.equals(column, that.column) && Objects.equals(referenced, that.referenced);
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, referenced);
  }
}
