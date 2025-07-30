/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

import com.databasepreservation.model.modules.configuration.enums.ExternalLobsAccessMethod;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class FileExternalLobsConfiguration extends ExternalLobsConfiguration {
  private String basePath;

  public FileExternalLobsConfiguration() {
    // empty constructor
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    FileExternalLobsConfiguration that = (FileExternalLobsConfiguration) o;
    return Objects.equals(basePath, that.basePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), basePath);
  }
}
