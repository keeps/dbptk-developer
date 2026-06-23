/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.databasepreservation.common.io.providers.PathInputStreamProvider;
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
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class ExternalLOBSCellHandlerS3AWS implements ExternalLOBSCellHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSCellHandlerS3AWS.class);

  private final S3Client s3Client;
  private final String bucketName;
  private final Reporter reporter;

  private final Path handlerTempDir;

  public ExternalLOBSCellHandlerS3AWS(String endpoint, String region, String bucketName, String accessKey,
    String secretKey, Path baseTempDirectory, Reporter reporter) {
    this.s3Client = S3Client.builder().endpointOverride(URI.create(endpoint)).forcePathStyle(true)
      .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey)).region(getRegion(region)).build();
    this.bucketName = bucketName;
    this.reporter = reporter;

    // Create a unique sub-directory for this specific handler/table
    this.handlerTempDir = baseTempDirectory.resolve("s3_handler_" + UUID.randomUUID());
    try {
      Files.createDirectories(this.handlerTempDir);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create isolated temp directory for S3 handler", e);
    }
  }

  private Region getRegion(String region) {
    return Region.of(region);
  }

  @Override
  public Cell handleCell(String cellId, String cellValue) throws ModuleException {
    Cell newCell = new NullCell(cellId);
    try {
      GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(cellValue).build();

      // Generate a unique path inside tempDirectory WITHOUT creating the file yet
      String uniqueFilename = "s3_lob_" + java.util.UUID.randomUUID() + ".bin";
      Path tempFile = this.handlerTempDir.resolve(uniqueFilename);

      // Download S3 object directly to the temporary file
      s3Client.getObject(getObjectRequest, ResponseTransformer.toFile(tempFile));

      // Use PathInputStreamProvider instead of InputStreamProviderImpl
      newCell = new BinaryCell(cellId, new PathInputStreamProvider(tempFile));
    } catch (S3Exception | SdkClientException e) {
      LOGGER.debug("Failed to obtain object from AWS bucket '{}': {}", bucketName, e.getMessage(), e);
      reporter.ignored("Cell " + cellId,
        "there was an error accessing the file in the bucket: '" + bucketName + "'; Cell Value: " + cellValue);
    } catch (Exception e) {
      LOGGER.debug("Unexpected error handling S3 object '{}': {}", bucketName, e.getMessage(), e);
      reporter.ignored("Cell " + cellId,
        "unexpected error accessing the file in the bucket: '" + bucketName + "'; Cell Value: " + cellValue);
    }
    return newCell;
  }

  @Override
  public void close() {
    s3Client.close();
  }
}
