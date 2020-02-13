/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common.providers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;

/**
 * An input stream provider serving streams from a temporary file, deleting the
 * file after use or before the JVM terminates.
 *
 * When passed an InputStream, copies the contents of an input stream to a
 * temporary file and provides InputStreams to read that file.
 *
 * The purpose of this class is to avoid having InputStreams open while they are
 * not being used, and to provide the streams when they are needed.
 *
 * Using ProvidesBlobInputStream is preferred, when possible.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class TemporaryPathInputStreamProvider extends PathInputStreamProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryPathInputStreamProvider.class);

  private String digest;
  private Thread removeTemporaryFileHook;

  /**
   * Copies the data from the inputStream to a temporary file and closes the
   * stream.
   *
   * @param inputStream
   *          the stream to be read, saved as a temporary file, and closed
   * @throws ModuleException
   *           if some IO problem happens. The stream is still closed.
   */
  public TemporaryPathInputStreamProvider(InputStream inputStream) throws ModuleException {
    try {
      path = Files.createTempFile("dbptk", "lob");
    } catch (IOException e) {
      try {
        inputStream.close();
      } catch (IOException e1) {
        LOGGER.debug("Could not close the stream after an error occurred trying to create the temporary file", e1);
      }
      throw new ModuleException().withMessage("Error creating temporary file").withCause(e);
    }

    try {
      DigestInputStream digestInputStream = new DigestInputStream(inputStream, MessageDigest.getInstance("MD5"));
      Files.copy(digestInputStream, path, StandardCopyOption.REPLACE_EXISTING);
      digest = DatatypeConverter.printHexBinary(digestInputStream.getMessageDigest().digest()).toUpperCase();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error copying stream to temp file").withCause(e);
    } catch (NoSuchAlgorithmException e) {
      throw new ModuleException().withMessage("Error trying to digest the stream").withCause(e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.debug("Could not close the stream after an error occurred", e);
      }
    }

    StringBuilder stackTraceBuilder = new StringBuilder();
    for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace()) {
      stackTraceBuilder.append(stackTraceElement.toString()).append(System.lineSeparator());
    }
    final String stackTrace = stackTraceBuilder.toString();

    removeTemporaryFileHook = new Thread() {
      @Override
      public void run() {
        TemporaryPathInputStreamProvider.this.removeTemporaryFileHook = null;
        TemporaryPathInputStreamProvider.this.cleanResources();
        throw new IllegalStateException("A PathInputStreamProvider was cleaned by a shutdown hook. Path: "
          + TemporaryPathInputStreamProvider.this.path.toAbsolutePath().toString() + ". Source: " + stackTrace);
      }
    };

    Runtime.getRuntime().addShutdownHook(removeTemporaryFileHook);
  }

  /**
   * Free all underlying resources, except for the stream itself.
   */
  @Override
  public void cleanResources() {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      LOGGER.debug("Could not delete temporary file", e);
    } finally {
      if (removeTemporaryFileHook != null) {
        Runtime.getRuntime().removeShutdownHook(removeTemporaryFileHook);
      }
    }
  }

  public String getDigest() {
    return this.digest;
  }
}
