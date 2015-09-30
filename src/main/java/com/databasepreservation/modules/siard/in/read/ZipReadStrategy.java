package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipReadStrategy implements ReadStrategy {
  private final HashMap<SIARDArchiveContainer, ZipFile> zipFiles;

  public ZipReadStrategy() {
    zipFiles = new HashMap<SIARDArchiveContainer, ZipFile>();
  }

  @Override
  public InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    InputStream stream = null;
    try {
      ZipFile zipFile = zipFiles.get(container);

      if (zipFile == null) {
        throw new IllegalStateException("Method 'setup' was not called for this container");
      }

      ZipArchiveEntry entry = zipFile.getEntry(path);

      if (entry == null) {
        throw new ModuleException(String.format("File \"%s\" is missing in container", path));
      }

      stream = zipFile.getInputStream(entry);
    } catch (IOException e) {
      throw new ModuleException(String.format("Error while accessing file \"%s\" in container", path), e);
    }
    return stream;
  }

  @Override
  public boolean isSimultaneousReadingSupported() {
    return true;
  }

  @Override
  public void finish(SIARDArchiveContainer container) throws ModuleException {
    try {
      ZipFile zipFile = zipFiles.get(container);

      if (zipFile == null) {
        throw new IllegalStateException("Method 'setup' was not called for this container");
      }

      zipFile.close();
    } catch (IOException e) {
      throw new ModuleException("Could not close zip file", e);
    }
  }

  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    try {
      if (zipFiles.containsKey(container)) {
        throw new IllegalStateException("Method 'setup' was already called for this container");
      }

      zipFiles.put(container, new ZipFile(container.getPath().toAbsolutePath().toString()));
    } catch (IOException e) {
      throw new ModuleException(String.format("Could not open zip file \"%s\"", container.getPath().toAbsolutePath()
        .toString()), e);
    }
  }

  @Override
  public CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container) throws ModuleException {
    ZipFile zipFile = zipFiles.get(container);

    if (zipFile == null) {
      throw new IllegalStateException("Method 'setup' was not called for this container");
    }

    final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();
    List<String> list = new ArrayList<String>();

    return new CloseableIterable<String>() {
      @Override
      public void close() throws IOException {
        // do nothing
      }

      @Override
      public Iterator<String> iterator() {
        return new Iterator<String>() {
          @Override
          public boolean hasNext() {
            return entries.hasMoreElements();
          }

          @Override
          public String next() {
            return entries.nextElement().getName();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException("remove() is not supported for this iterator");
          }
        };
      }
    };
  }
}
