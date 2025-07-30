package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class S3MinIOExternalLobsConfiguration extends ExternalLobsConfiguration {

  private String endpoint;
  private String bucketName;
  private String accessKey;
  private String secretKey;

  public S3MinIOExternalLobsConfiguration() {
    super();
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    S3MinIOExternalLobsConfiguration that = (S3MinIOExternalLobsConfiguration) o;
    return Objects.equals(endpoint, that.endpoint) && Objects.equals(bucketName, that.bucketName)
      && Objects.equals(accessKey, that.accessKey) && Objects.equals(secretKey, that.secretKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), endpoint, bucketName, accessKey, secretKey);
  }
}
