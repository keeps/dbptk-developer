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
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final int MAX_NETWORK_RETRIES = 3;
  private static final int BASE_POLLING_INTERVAL_MS = 2000;
  private static final int MAX_POLLING_ATTEMPTS = 600; // ~20 minutes maximum wait per file before assuming Zombie Job

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
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(60)).build();
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

    String header = "--" + boundary + "\r\n" + "Content-Disposition: form-data; name=\"targetFormat\"\r\n\r\n"
      + targetFormat + "\r\n" + "--" + boundary + "\r\n"
      + "Content-Disposition: form-data; name=\"file\"; filename=\"lob_" + cellId + ".bin\"\r\n"
      + "Content-Type: application/octet-stream\r\n\r\n";

    String footer = "\r\n--" + boundary + "--\r\n";

    InputStream headerStream = new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8));
    InputStream footerStream = new ByteArrayInputStream(footer.getBytes(StandardCharsets.UTF_8));

    InputStream multipartStream = new SequenceInputStream(
      Collections.enumeration(Arrays.asList(headerStream, inputStream, footerStream)));

    HttpRequest submitRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs"))
      .header("Content-Type", "multipart/form-data; boundary=" + boundary)
      .POST(BodyPublishers.ofInputStream(() -> multipartStream)).build();

    HttpResponse<String> submitResponse = httpClient.send(submitRequest, BodyHandlers.ofString());

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

      JobStatusResponse status = objectMapper.readValue(statusResponse.body(), JobStatusResponse.class);

      switch (status.status().toUpperCase()) {
        case "COMPLETED", "DONE", "SUCCESS" -> {
          log.debug("Job {} (Cell {}) completed successfully after {} attempts.", jobId, cellId, attempts);
          return;
        }
        case "FAILED", "ERROR", "EVICTED" -> {
          log.error("API reported terminal failure for Job {} (Cell {})", jobId, cellId);
          throw new RuntimeException("Server failed to convert cell: " + cellId);
        }
        default -> {
          if (attempts % 30 == 0) {
            log.warn("Job {} (Cell {}) is taking unusually long. Current status: {}. Attempt: {}/{}", jobId, cellId,
              status.status(), attempts, MAX_POLLING_ATTEMPTS);
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

    Path convertedLob = null;
    Path reportFile = null;

    try (ZipInputStream zis = new ZipInputStream(new FileInputStream(tempZipFile.toFile()))) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        Path extractedFilePath = extractionDir.resolve(zipEntry.getName());

        if (!extractedFilePath.normalize().startsWith(extractionDir)) {
          throw new SecurityException("Corrupted ZIP entry: " + zipEntry.getName());
        }

        if (!zipEntry.isDirectory()) {
          Files.copy(zis, extractedFilePath);

          if (zipEntry.getName().toLowerCase().contains("report")) {
            reportFile = extractedFilePath;
          } else {
            convertedLob = extractedFilePath;
          }
        }
      }
    }

    if (convertedLob == null || reportFile == null) {
      throw new RuntimeException("Downloaded ZIP lacks expected format (LOB + Report) for cell: " + cellId);
    }

    return new ConversionResult(convertedLob, reportFile);
  }

  private <T> HttpResponse<T> executeWithRetry(HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler,
    int maxRetries) throws Exception {
    Exception lastException = null;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return httpClient.send(request, responseBodyHandler);
      } catch (IOException e) {
        lastException = e;
        log.warn("Attempt {} failed for {}: {}", attempt, request.uri(), e.getMessage());

        if (attempt == maxRetries)
          break;
        Thread.sleep((long) Math.pow(2, attempt) * 1000);
      }
    }
    throw new IOException("Exhausted all network retries for URI: " + request.uri(), lastException);
  }
}