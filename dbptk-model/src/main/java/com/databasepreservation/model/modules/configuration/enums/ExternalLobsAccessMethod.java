package com.databasepreservation.model.modules.configuration.enums;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum ExternalLobsAccessMethod {
  FILE_SYSTEM("file-system"), REMOTE("remote-ssh");

  private final String value;

  ExternalLobsAccessMethod(String value) {
    this.value = value;
  }

  @JsonValue
  public String getValue() {
    return value;
  }
}
