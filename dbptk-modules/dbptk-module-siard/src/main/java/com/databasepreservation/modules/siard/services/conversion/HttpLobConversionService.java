package com.databasepreservation.modules.siard.services.conversion;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.siard.services.conversion.model.ConversionResult;
import com.databasepreservation.modules.siard.services.conversion.model.JobStatus;
import com.databasepreservation.modules.siard.services.conversion.model.JobStatusResponse;
import com.databasepreservation.modules.siard.services.conversion.model.JobSubmissionResponse;
import com.databasepreservation.utils.ConfigUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Handles communication with the external format conversion REST API. Uses
 * asynchronous polling and in-memory multipart streaming to process large
 * objects efficiently.
 *
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class HttpLobConversionService implements LobConversionService {

  private static final Logger log = LoggerFactory.getLogger(HttpLobConversionService.class);

  private static final int MAX_NETWORK_RETRIES = ConfigUtils.getProperty(3,
    "dbptk.service.lob.conversion.networkRetries");
  private static final int BASE_POLLING_INTERVAL_MS = ConfigUtils.getProperty(2000,
    "dbptk.service.lob.conversion.basePollingIntervalMs");
  private static final int MAX_POLLING_ATTEMPTS = ConfigUtils.getProperty(600,
    "dbptk.service.lob.conversion.maxPollingAttempts");
  private static final int CONNECTION_TIMEOUT_SECONDS = ConfigUtils.getProperty(60,
    "dbptk.service.lob.conversion.connectionTimeoutSeconds");

  private static final String CRLF = "\r\n";

  private final HttpClient httpClient;
  private final String baseUrl;
  private final String targetFormat;
  private final ObjectMapper objectMapper;
  private final TempFileTracker fileTracker;
  private final Random random = new Random();

  public HttpLobConversionService(String baseUrl, String targetFormat, TempFileTracker fileTracker) {
    this.baseUrl = baseUrl;
    this.targetFormat = targetFormat;
    this.fileTracker = fileTracker;
    this.objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS)).build();
  }

  @Override
  public ConversionResult convertLob(String cellId, InputStream inputStream) throws Exception {
    log.debug("Initiating conversion pipeline for cell: {}", cellId);
    String jobId = submitJob(cellId, inputStream);
    waitForCompletion(cellId, jobId);
    return downloadAndExtractResult(cellId, jobId);
  }

  private String submitJob(String cellId, InputStream inputStream) throws Exception {
    String boundary = "DbptkBoundary" + System.currentTimeMillis();

    String header = buildMultipartHeader(boundary, cellId);
    String footer = CRLF + "--" + boundary + "--" + CRLF;

    InputStream headerStream = new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8));
    InputStream footerStream = new ByteArrayInputStream(footer.getBytes(StandardCharsets.UTF_8));

    InputStream multipartStream = new SequenceInputStream(new SequenceInputStream(headerStream, inputStream),
      footerStream);

    HttpRequest submitRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs"))
      .header("Content-Type", "multipart/form-data; boundary=" + boundary)
      .POST(BodyPublishers.ofInputStream(() -> multipartStream)).build();

    HttpResponse<String> submitResponse = executeWithRetry(submitRequest, BodyHandlers.ofString(), MAX_NETWORK_RETRIES);

    if (submitResponse.statusCode() >= 400) {
      log.error("API rejected LOB submission for cell {}. Status: {}, Body: {}", cellId, submitResponse.statusCode(),
        submitResponse.body());
      throw new RuntimeException("Failed to submit LOB for cell " + cellId);
    }

    JobSubmissionResponse job = objectMapper.readValue(submitResponse.body(), JobSubmissionResponse.class);
    log.debug("Successfully dispatched cell {}. Assigned Job ID: {}", cellId, job.id());
    return job.id();
  }

  private void waitForCompletion(String cellId, String jobId) throws Exception {
    log.debug("Awaiting completion of Job {} (Cell {})", jobId, cellId);

    for (int attempts = 1; attempts <= MAX_POLLING_ATTEMPTS; attempts++) {
      HttpRequest statusRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs/" + jobId)).GET().build();
      HttpResponse<String> statusResponse = executeWithRetry(statusRequest, BodyHandlers.ofString(),
        MAX_NETWORK_RETRIES);

      JobStatusResponse response = objectMapper.readValue(statusResponse.body(), JobStatusResponse.class);

      switch (response.status()) {
        case JobStatus.COMPLETED -> {
          log.debug("Job {} (Cell {}) completed successfully after {} attempts.", jobId, cellId, attempts);
          return;
        }
        case JobStatus.FAILED, JobStatus.EVICTED -> {
          log.error("API reported terminal failure for Job {} (Cell {}) with status: {}", jobId, cellId,
            response.status());
          throw new RuntimeException(
            "Server failed to convert cell: " + cellId + " (Status: " + response.status() + ")");
        }
        case JobStatus.ACCEPTED, JobStatus.PROCESSING -> {
          if (attempts % 30 == 0) {
            log.warn("Job {} (Cell {}) is taking unusually long. Current status: {}. Attempt: {}/{}", jobId, cellId,
              response.status(), attempts, MAX_POLLING_ATTEMPTS);
          }
          long sleepTime = BASE_POLLING_INTERVAL_MS + random.nextInt(1000);
          Thread.sleep(sleepTime);
        }
      }
    }
    log.error("Zombie Job detected. API failed to resolve Job {} (Cell {}) within the maximum polling threshold.",
      jobId, cellId);
    throw new RuntimeException("Timeout after waiting for conversion of cell: " + cellId);
  }

  /**
   * Downloads the resulting ZIP and extracts its contents, freeing the ZIP file
   * immediately after.
   */
  private ConversionResult downloadAndExtractResult(String cellId, String jobId) throws Exception {
    Path tempZipFile = Files.createTempFile("siarddk_conv_" + cellId + "_", ".zip");
    fileTracker.track(tempZipFile);

    try {
      HttpRequest downloadRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs/" + jobId + "/download"))
        .GET().build();

      executeWithRetry(downloadRequest, BodyHandlers.ofFile(tempZipFile), MAX_NETWORK_RETRIES);

      return extractZipContents(cellId, tempZipFile);
    } finally {
      // Free disk space immediately after extraction
      fileTracker.deleteEarly(tempZipFile);
    }
  }

  private ConversionResult extractZipContents(String cellId, Path tempZipFile) throws Exception {
    Path extractionDir = Files.createTempDirectory("siarddk_extracted_" + cellId + "_");
    fileTracker.trackDir(extractionDir);

    Path normalizedExtractionDir = extractionDir.normalize();

    List<Path> convertedFiles = new ArrayList<>();
    Path reportFile = null;

    try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZipFile.toFile()))) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        Path extractedFilePath = extractionDir.resolve(zipEntry.getName()).normalize();

        if (!extractedFilePath.startsWith(normalizedExtractionDir)) {
          throw new SecurityException("Corrupted ZIP entry (Zip Slip vulnerability detected): " + zipEntry.getName());
        }

        if (!zipEntry.isDirectory()) {
          Files.copy(zis, extractedFilePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

          if (zipEntry.getName().toLowerCase().contains("report")) {
            reportFile = extractedFilePath;
          } else {
            convertedFiles.add(extractedFilePath);
          }
        }
      }
    }

    if (convertedFiles.isEmpty() || reportFile == null) {
      throw new RuntimeException("Downloaded ZIP lacks expected format (at least 1 LOB + Report) for cell: " + cellId);
    }

    return new ConversionResult(convertedFiles, reportFile);
  }

  private <T> HttpResponse<T> executeWithRetry(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler,
    int maxRetries) throws Exception {
    Exception lastException = null;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        HttpResponse<T> response = httpClient.send(request, responseBodyHandler);

        if (response.statusCode() >= 500) {
          throw new IOException("Temporary server error: " + response.statusCode() + " - " + response.body());
        }

        return response;
      } catch (IOException e) {
        lastException = e;
        log.warn("Attempt {} failed for {}: {}", attempt, request.uri(), e.getMessage());

        if (attempt == maxRetries)
          break;

        try {
          Thread.sleep((long) Math.pow(2, attempt) * 1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Retry interrupted for URI: " + request.uri(), ie);
        }
      }
    }
    throw new IOException("Exhausted all network retries for URI: " + request.uri(), lastException);
  }

  private String buildMultipartHeader(String boundary, String cellId) {
    StringBuilder sb = new StringBuilder();

    // Target format part header
    sb.append("--").append(boundary).append(CRLF);
    sb.append("Content-Disposition: form-data; name=\"targetFormat\"").append(CRLF);
    sb.append(CRLF);
    sb.append(this.targetFormat).append(CRLF);

    // File part header
    sb.append("--").append(boundary).append(CRLF);
    sb.append("Content-Disposition: form-data; name=\"file\"; filename=\"lob_").append(cellId).append(".bin\"")
      .append(CRLF);
    sb.append("Content-Type: application/octet-stream").append(CRLF);
    sb.append(CRLF);

    return sb.toString();
  }
}