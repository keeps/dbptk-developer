package com.databasepreservation.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;

/**
 * An input stream provider serving streams from a file.
 *
 * When passed an InputStream, copies the contents of an input stream to a
 * temporary file and provides InputStreams to read that file.
 *
 * When passed a Path, provides InputStreams to read the file at that path.
 *
 * The purpose of this class is to avoid having InputStreams open while they are
 * not being used, and to provide the streams when they are needed.
 *
 * Using ProvidesBlobInputStream is preferred, when possible.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class PathInputStreamProvider implements InputStreamProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PathInputStreamProvider.class);

  private final Path path;
  private Thread removeTemporaryFileHook = null;

  /**
   * Copies the data from the inputStream to a temporary file and closes the
   * stream.
   *
   * @param inputStream
   *          the stream to be read, saved as a temporary file, and closed
   * @throws ModuleException
   *           if some IO problem happens. The stream is still closed.
   */
  public PathInputStreamProvider(InputStream inputStream) throws ModuleException {
    try {
      path = Files.createTempFile("dbptk", "lob");
    } catch (IOException e) {
      try {
        inputStream.close();
      } catch (IOException e1) {
        LOGGER.debug("Could not close the stream after an error occurred trying to create the temporary file", e1);
      }
      throw new ModuleException("Error creating temporary file", e);
    }

    try {
      Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new ModuleException("Error copying stream to temp file", e);
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.debug("Could not close the stream after an error occurred", e);
      }
    }

    removeTemporaryFileHook = new Thread() {
      @Override
      public void run() {
        LOGGER.debug("A PathInputStreamProvider was cleaned by a shutdown hook. Path: "
          + PathInputStreamProvider.this.path.toAbsolutePath().toString());
        PathInputStreamProvider.this.removeTemporaryFileHook = null;
        PathInputStreamProvider.this.cleanResources();
      }
    };

    Runtime.getRuntime().addShutdownHook(removeTemporaryFileHook);
  }

  /**
   * Use the specified file path to create the streams.
   *
   * @param fileLocation
   *          the stream to be read, saved as a temporary file, and closed
   * @throws ModuleException
   *           if some IO problem happens. The stream is still closed.
   */
  public PathInputStreamProvider(Path fileLocation) throws ModuleException {
//    if (Files.isReadable(fileLocation)) {
//      throw new ModuleException("Path " + fileLocation.toAbsolutePath().toString() + " is not readable.");
//    }
    this.path = fileLocation;
    removeTemporaryFileHook = null;
  }

  /**
   * Create a new input stream to read data
   * <p>
   * Contract: The stream must be closed elsewhere. It is not closed
   * automatically in any way, not even by cleanResources
   *
   * @return the new input stream
   * @throws ModuleException
   *           if the input stream could not be created
   */
  @Override
  public InputStream createInputStream() throws ModuleException {
    try {
      return Files.newInputStream(path);
    } catch (IOException e) {
      throw new ModuleException("Could not create an input stream", e);
    }
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

  @Override
  public long getSize() throws ModuleException {
    try {
      return Files.size(path);
    } catch (IOException e) {
      throw new ModuleException("Could not get lob file size", e);
    }
  }
}
