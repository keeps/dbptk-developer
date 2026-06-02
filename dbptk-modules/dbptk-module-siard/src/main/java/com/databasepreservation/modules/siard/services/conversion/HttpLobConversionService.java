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
 */
public class HttpLobConversionService implements LobConversionService {

  private static final Logger log = LoggerFactory.getLogger(HttpLobConversionService.class);

  private static final int MAX_POLLING_ATTEMPTS = 300;
  private static final int MAX_NETWORK_RETRIES = 3;
  private static final int BASE_POLLING_INTERVAL_MS = 2000;

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

    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  @Override
  public ConversionResult convertLob(String cellId, InputStream inputStream) throws Exception {
    String jobId = submitJob(cellId, inputStream);
    waitForCompletion(cellId, jobId);
    return downloadAndExtractResult(cellId, jobId);
  }

  /**
   * Submits the job using a SequenceInputStream to stream the multipart request
   * directly, avoiding intermediate disk writes for the payload.
   */
  private String submitJob(String cellId, InputStream inputStream) throws Exception {
    String boundary = "DbptkBoundary" + System.currentTimeMillis();

    String header = "--" + boundary + "\r\n" + "Content-Disposition: form-data; name=\"targetFormat\"\r\n\r\n"
      + targetFormat + "\r\n" + "--" + boundary + "\r\n"
      + "Content-Disposition: form-data; name=\"file\"; filename=\"lob_" + cellId + ".bin\"\r\n"
      + "Content-Type: application/octet-stream\r\n\r\n";

    String footer = "\r\n--" + boundary + "--\r\n";

    InputStream headerStream = new ByteArrayInputStream(header.getBytes(StandardCharsets.UTF_8));
    InputStream footerStream = new ByteArrayInputStream(footer.getBytes(StandardCharsets.UTF_8));

    // Chains the header, actual LOB data, and footer without loading the LOB into
    // memory
    InputStream multipartStream = new SequenceInputStream(
      Collections.enumeration(Arrays.asList(headerStream, inputStream, footerStream)));

    HttpRequest submitRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs"))
      .header("Content-Type", "multipart/form-data; boundary=" + boundary)
      .POST(BodyPublishers.ofInputStream(() -> multipartStream)).build();

    // Not using executeWithRetry here because the InputStream is consumed and
    // cannot be trivially reset.
    HttpResponse<String> submitResponse = httpClient.send(submitRequest, BodyHandlers.ofString());

    if (submitResponse.statusCode() >= 400) {
      throw new RuntimeException("Failed to submit LOB for cell " + cellId + ": " + submitResponse.body());
    }

    JobSubmissionResponse job = objectMapper.readValue(submitResponse.body(), JobSubmissionResponse.class);
    return job.id();
  }

  /**
   * Polls the job status API until completion or timeout. Includes jitter to
   * prevent thundering herd.
   */
  private void waitForCompletion(String cellId, String jobId) throws Exception {
    for (int i = 0; i < MAX_POLLING_ATTEMPTS; i++) {
      HttpRequest statusRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs/" + jobId)).GET().build();

      HttpResponse<String> statusResponse = executeWithRetry(statusRequest, BodyHandlers.ofString(),
        MAX_NETWORK_RETRIES);
      JobStatusResponse status = objectMapper.readValue(statusResponse.body(), JobStatusResponse.class);

      switch (status.status().toUpperCase()) {
        case "COMPLETED", "DONE", "SUCCESS" -> {
          return;
        }
        case "FAILED", "ERROR", "EVICTED" -> throw new RuntimeException("Server failed to convert cell: " + cellId);
        default -> {
          long sleepTime = BASE_POLLING_INTERVAL_MS + random.nextInt(1000); // Jittering
          Thread.sleep(sleepTime);
        }
      }
    }
    throw new RuntimeException("Timeout after waiting for conversion of cell: " + cellId);
  }

  /**
   * Downloads the resulting ZIP and extracts its contents.
   */
  private ConversionResult downloadAndExtractResult(String cellId, String jobId) throws Exception {
    Path tempZipFile = Files.createTempFile("siarddk_conv_" + cellId + "_", ".zip");
    fileTracker.track(tempZipFile);

    HttpRequest downloadRequest = HttpRequest.newBuilder().uri(URI.create(baseUrl + "/jobs/" + jobId + "/download"))
      .GET().build();

    executeWithRetry(downloadRequest, BodyHandlers.ofFile(tempZipFile), MAX_NETWORK_RETRIES);

    return extractZipContents(cellId, tempZipFile);
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

        // Zip Slip vulnerability prevention
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

  /**
   * Enforces resilient networking via exponential backoff. Intended exclusively
   * for idempotent requests (e.g., GET).
   */
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
        Thread.sleep((long) Math.pow(2, attempt) * 1000); // Exponential backoff: 2s, 4s, 8s...
      }
    }
    throw new IOException("Exhausted all network retries for URI: " + request.uri(), lastException);
  }
}