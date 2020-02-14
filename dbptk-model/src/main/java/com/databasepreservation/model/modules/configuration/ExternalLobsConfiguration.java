package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

import com.databasepreservation.model.modules.configuration.enums.ExternalLobsAccessMethod;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"basePath", "accessMethod"})
public class ExternalLobsConfiguration {
  private String basePath;
  private ExternalLobsAccessMethod accessModule;

  public ExternalLobsConfiguration() {
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  @JsonProperty("accessMethod")
  public ExternalLobsAccessMethod getAccessModule() {
    return accessModule;
  }

  public void setAccessModule(ExternalLobsAccessMethod accessModule) {
    this.accessModule = accessModule;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ExternalLobsConfiguration that = (ExternalLobsConfiguration) o;
    return Objects.equals(getBasePath(), that.getBasePath()) && getAccessModule() == that.getAccessModule();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getBasePath(), getAccessModule());
  }
}
