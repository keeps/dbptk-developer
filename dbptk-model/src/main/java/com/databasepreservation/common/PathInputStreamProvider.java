package com.databasepreservation.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import com.databasepreservation.model.exception.ModuleException;

/**
 * An input stream provider serving streams from an existing file.
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

  protected Path path;

  protected PathInputStreamProvider() {
    // do nothing, used by subclasses
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
    if (!Files.isReadable(fileLocation)) {
      throw new ModuleException("Path " + fileLocation.toAbsolutePath().toString() + " is not readable.");
    }
    this.path = fileLocation;
  }

  /**
   * Create a new input stream to read data
   * <p>
   * Contract: The stream must be closed elsewhere. It is not closed automatically
   * in any way, not even by cleanResources
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
    // nothing to do here
  }

  @Override
  public long getSize() throws ModuleException {
    try {
      return Files.size(path);
    } catch (IOException e) {
      throw new ModuleException("Could not get file size", e);
    }
  }
}
