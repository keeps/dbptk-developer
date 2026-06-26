package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;

import com.databasepreservation.common.io.providers.CompletableFutureInputStreamProvider;
import com.databasepreservation.modules.externalLobs.ExternalLOBSFilter;
import com.databasepreservation.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.PathInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class ExternalLOBSCellHandlerS3AWS implements ExternalLOBSCellHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSCellHandlerS3AWS.class);

  private final S3AsyncClient s3AsyncClient;
  private final S3TransferManager transferManager;
  private final String bucketName;
  private final Reporter reporter;

  public ExternalLOBSCellHandlerS3AWS(String endpoint, String region, String bucketName, String accessKey,
    String secretKey, Reporter reporter) {

    S3CrtRetryConfiguration retryConfig = S3CrtRetryConfiguration.builder()
      .numRetries(ConfigUtils.getProperty(5, "dbptk.external-lobs-filter.s3.max-retries")).build();

    this.s3AsyncClient = S3AsyncClient.crtBuilder().endpointOverride(URI.create(endpoint)).forcePathStyle(true)
      .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey)).region(getRegion(region))
      .maxConcurrency(ConfigUtils.getProperty(200, "dbptk.external-lobs-filter.s3-max-concurrency"))
      .retryConfiguration(retryConfig).build();

    this.transferManager = S3TransferManager.builder().s3Client(this.s3AsyncClient).build();

    this.bucketName = bucketName;
    this.reporter = reporter;
  }

  private Region getRegion(String region) {
    return Region.of(region);
  }

  @Override
  public Cell handleCell(String cellId, String cellValue) throws ModuleException {
    try {
      Path tempFile = Files.createTempFile("s3-lob-" + cellId + "-", ".tmp");

      // Register the file to the ThreadLocal so the Consumer thread deletes it later
      ExternalLOBSFilter.THREAD_TEMP_FILES.get().add(tempFile);

      DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
        .getObjectRequest(b -> b.bucket(bucketName).key(cellValue)).destination(tempFile).build();

      FileDownload download = transferManager.downloadFile(downloadFileRequest);

      return new BinaryCell(cellId, new CompletableFutureInputStreamProvider(download.completionFuture(), tempFile));
    } catch (CompletionException ce) {
      Throwable cause = ce.getCause();
      LOGGER.debug("Failed to obtain async object from AWS bucket '{}': {}", bucketName, cause.getMessage(), cause);
      reporter.ignored("Cell " + cellId, "there was an error asynchronously accessing the file in the bucket: '"
        + bucketName + "'; Cell Value: " + cellValue);
    } catch (Exception e) {
      LOGGER.debug("Unexpected error initiating download from AWS bucket '{}': {}", bucketName, e.getMessage(), e);
      reporter.ignored("Cell " + cellId,
        "unexpected file system or SDK error while creating download request; Cell Value: " + cellValue);
    }

    return new NullCell(cellId);
  }
}