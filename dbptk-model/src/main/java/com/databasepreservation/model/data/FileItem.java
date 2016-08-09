/**
 *
 */
package com.databasepreservation.model.data;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Luis Faria
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class FileItem {
  private final Path path;

  private ArrayList<InputStream> createdStreams;

  /**
   * File item constructor, copying inputstream to item
   *
   * @param inputStream
   *          the source input stream
   * @throws ModuleException
   */
  public FileItem(InputStream inputStream) throws ModuleException {
    try {
      path = Files.createTempFile("dbptk", "lob");
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          FileItem.this.deleteSilently();
        }
      });
    } catch (IOException e) {
      throw new ModuleException("Error creating temporary file", e);
    }

    try {
      Files.copy(inputStream, path, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new ModuleException("Error copying stream to temp file", e);
    }

  }

  /**
   * Creates an input stream to retrieve the content from the file item
   *
   * @return an input stream
   * @throws ModuleException
   */
  public InputStream createInputStream() throws ModuleException {
    if (createdStreams == null) {
      createdStreams = new ArrayList<>();
    }

    InputStream newStream;
    try {
      newStream = Files.newInputStream(path);
    } catch (FileNotFoundException e) {
      throw new ModuleException("Error getting input stream from temp file", e);
    } catch (IOException e) {
      throw new ModuleException("Error getting input stream from temp file", e);
    }
    createdStreams.add(newStream);
    return newStream;
  }

  /**
   * The a file with the contents of the file item
   *
   * @return a file
   */
  public File getFile() {
    return new File(path.toAbsolutePath().toString());
  }

  /**
   * Get the size of the contents of the file item
   *
   * @return the size
   */
  public long size() throws ModuleException {
    try {
      return Files.size(path);
    } catch (IOException e) {
      throw new ModuleException("Error getting size of temp file", e);
    }
  }

  /**
   * Close all created streams and delete the file
   *
   * @return true if file item successfully deleted, false otherwise
   */
  public void delete() throws IOException {
    if (createdStreams != null) {
      for (InputStream stream : createdStreams) {
        stream.close();
      }
    }
    Files.delete(path);
  }

  public void deleteSilently() {
    try {
      delete();
    } catch (IOException e) {
      // ignore
    }
  }

  @Override
  public String toString() {
    return "FileItem{" + "path=" + path + ", createdStreams=" + createdStreams + '}';
  }
}
