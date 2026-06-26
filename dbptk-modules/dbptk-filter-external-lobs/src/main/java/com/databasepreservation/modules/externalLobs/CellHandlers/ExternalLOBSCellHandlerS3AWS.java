/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.net.URI;

import com.databasepreservation.common.io.providers.S3InputStreamProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class ExternalLOBSCellHandlerS3AWS implements ExternalLOBSCellHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSCellHandlerS3AWS.class);

  private final S3Client s3Client;
  private final String bucketName;
  private final Reporter reporter;

  public ExternalLOBSCellHandlerS3AWS(String endpoint, String region, String bucketName, String accessKey,
    String secretKey, Reporter reporter) {
    this.s3Client = S3Client.builder().endpointOverride(URI.create(endpoint)).forcePathStyle(true)
      .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey)).region(getRegion(region))
      .httpClientBuilder(UrlConnectionHttpClient.builder()).build();
    this.bucketName = bucketName;
    this.reporter = reporter;
  }

  private Region getRegion(String region) {
    return Region.of(region);
  }

  @Override
  public Cell handleCell(String cellId, String cellValue) throws ModuleException {
    try {
      // 1. Fetch metadata to get the file size (Fast, no payload download)
      HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucketName).key(cellValue).build();

      HeadObjectResponse headResponse = s3Client.headObject(headRequest);
      long fileSize = headResponse.contentLength();

      // 2. Create the Lazy-Loading Provider with the size
      S3InputStreamProvider provider = new S3InputStreamProvider(s3Client, bucketName, cellValue, fileSize);

      // 3. Return the cell
      return new BinaryCell(cellId, provider);

    } catch (S3Exception | SdkClientException e) {
      LOGGER.debug("Failed to obtain metadata from AWS bucket '{}': {}", bucketName, e.getMessage(), e);
      reporter.ignored("Cell " + cellId,
        "error accessing metadata in bucket: '" + bucketName + "'; Cell Value: " + cellValue);
      return new NullCell(cellId);
    }
  }

  @Override
  public void close() {
    s3Client.close();
  }
}