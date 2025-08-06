/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class S3AWSExternalLobsConfiguration extends ExternalLobsConfiguration {

  private String endpoint;
  private String bucketName;
  private String region;
  private String accessKey;
  private String secretKey;

  public S3AWSExternalLobsConfiguration() {
    super();
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
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
    S3AWSExternalLobsConfiguration that = (S3AWSExternalLobsConfiguration) o;
    return Objects.equals(bucketName, that.bucketName) && Objects.equals(region, that.region);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), bucketName, region);
  }
}
