/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.read;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipSegmentedReadStrategy implements ReadStrategy {
  private final HashMap<SIARDArchiveContainer, ZipFile> zipFiles;

  public ZipSegmentedReadStrategy() {
    zipFiles = new HashMap<>();
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
        try {
          Path filePath = container.getPath().resolve(Paths.get(path));
          if (Files.exists(filePath)) {
            return Files.newInputStream(container.getPath().resolve(Paths.get(path)));
          } else {
            Path fileParthPath = filePath.resolve("_part001");
            if (Files.exists(fileParthPath)) {
              String[] fileParthPathSplit = fileParthPath.toString().split(File.separator);
              int initialSegment = Integer
                      .parseInt(fileParthPathSplit[fileParthPathSplit.length - 2].replaceAll("[^0-9]", ""));
              InputStream sequenceStream = Files.newInputStream(container.getPath().resolve(fileParthPath));
              for (int segment = initialSegment; Files.exists(fileParthPath); segment++) {
                for (int part = 0; Files.exists(fileParthPath); part++) {
                  String segmentPartPath = fileParthPath.toString().replace("_part001",
                          "_part" + String.format("%03d", part + 1));
                  if (Files.exists(Paths.get(segmentPartPath))) {
                    InputStream partStream = Files.newInputStream(Paths.get(segmentPartPath));
                    sequenceStream = new SequenceInputStream(sequenceStream, partStream);
                  } else {
                    break;
                  }
                }
              }
            } else {
              throw new IOException("Couldn't find file");
            }
          }
        } catch (IOException e) {
          throw new ModuleException().withMessage(String.format("File \"%s\" is missing in container", path));
        }
      }

      stream = zipFile.getInputStream(entry);
    } catch (IOException e) {
      throw new ModuleException().withMessage(String.format("Error while accessing file \"%s\" in container", path))
        .withCause(e);
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
      throw new ModuleException().withMessage("Could not close zip file").withCause(e);
    }
  }

  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    try {
      if (!zipFiles.containsKey(container)) {
        zipFiles.put(container, new ZipFile(container.getPath().toAbsolutePath().toString()));
      }

      if (container.getVersion() == null) {
        container.setVersion(MetadataPathStrategy.VersionIdentifier.getVersion(this, container));
      }
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage(String.format("Could not open zip file \"%s\"", container.getPath().toAbsolutePath().toString()))
        .withCause(e);
    }
  }

  @Override
  public CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container) throws ModuleException {
    ZipFile zipFile = zipFiles.get(container);

    if (zipFile == null) {
      throw new IllegalStateException("Method 'setup' was not called for this container");
    }

    final Enumeration<ZipArchiveEntry> entries = zipFile.getEntries();

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
