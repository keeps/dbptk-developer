package com.databasepreservation.modules.externalLobs.CellHandlers;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;

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
    this.s3AsyncClient = S3AsyncClient.builder().endpointOverride(URI.create(endpoint)).forcePathStyle(true)
      .credentialsProvider(() -> AwsBasicCredentials.create(accessKey, secretKey)).region(getRegion(region)).build();

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
      // Stream LOB straight to a temporary physical file
      Path tempFile = Files.createTempFile("s3-lob-" + cellId + "-", ".tmp");

      DownloadFileRequest downloadFileRequest = DownloadFileRequest.builder()
        .getObjectRequest(b -> b.bucket(bucketName).key(cellValue)).destination(tempFile).build();

      FileDownload download = transferManager.downloadFile(downloadFileRequest);

      // Wait for the async download inside this Virtual Thread boundary to finish
      download.completionFuture().join();

      return new BinaryCell(cellId, new PathInputStreamProvider(tempFile));

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