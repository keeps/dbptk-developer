/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXTERNAL_PROPERTY, property = "accessMethod", visible = true)
@JsonSubTypes({@JsonSubTypes.Type(value = FileExternalLobsConfiguration.class, name = "file-system"),
  @JsonSubTypes.Type(value = RemoteExternalLobsConfiguration.class, name = "remote-ssh"),
  @JsonSubTypes.Type(value = S3MinIOExternalLobsConfiguration.class, name = "S3-MinIO"),
  @JsonSubTypes.Type(value = S3AWSExternalLobsConfiguration.class, name = "S3-AWS")})

public class ExternalLobsConfiguration {
  @JsonProperty("accessMethod")
  private String accessMethod;

  public ExternalLobsConfiguration() {
    // empty constructor
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass())
      return false;
    ExternalLobsConfiguration that = (ExternalLobsConfiguration) o;
    return Objects.equals(accessMethod, that.accessMethod);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(accessMethod);
  }
}
