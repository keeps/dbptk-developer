package com.databasepreservation.model.modules.configuration.enums;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum DatabaseMetadata {
  ROUTINES("routines"), TRIGGERS("triggers"), USERS("users"), ROLES("roles"), PRIVILEGES("privileges"),
  PRIMARY_KEYS("primaryKeys"), CANDIDATE_KEYS("candidateKeys"), FOREIGN_KEYS("foreignKeys"),
  CHECK_CONSTRAINTS("checkConstraints"), VIEWS("views");

  private final String value;

  DatabaseMetadata(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }
}