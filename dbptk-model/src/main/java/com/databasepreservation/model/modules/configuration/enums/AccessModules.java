package com.databasepreservation.model.modules.configuration.enums;

import com.databasepreservation.Constants;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum AccessModules {
  FILE_SYSTEM("file"), VIEWS("ssh");

  private final String value;

  AccessModules(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }
}
