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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * Read strategy that uses a ZipReadStrategy to read from the main SIARD archive
 * container and a folder read strategy to read from the auxiliary SIARD archive
 * containers.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipAndFolderSegmentedReadStrategy implements ReadStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipAndFolderSegmentedReadStrategy.class);
  private final ZipSegmentedReadStrategy zipRead;
  private final SIARDArchiveContainer mainContainer;

  public ZipAndFolderSegmentedReadStrategy(SIARDArchiveContainer mainContainer) {
    zipRead = new ZipSegmentedReadStrategy();
    this.mainContainer = mainContainer;
  }

  @Override
  public InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    if (container == mainContainer) {
      return zipRead.createInputStream(container, path);
    } else {
      try {
        Path filePath = container.getPath().resolve(Paths.get(path));
        if (Files.exists(filePath)) {
          return Files.newInputStream(container.getPath().resolve(Paths.get(path)));
        } else {
          Path filePartPath = filePath.resolve("_part001");
          if (Files.exists(filePartPath)) {
            String[] filePartPathSplit = filePartPath.toString().split(File.separator);
            String pathWithoutSegment = String.join(File.separator,
              java.util.Arrays.copyOf(filePartPathSplit, filePartPathSplit.length - 2));
            int initialSegment = Integer
              .parseInt(filePartPathSplit[filePartPathSplit.length - 2].replaceAll("[^0-9]", ""));

            InputStream sequenceStream = Files.newInputStream(container.getPath().resolve(filePartPath));

            int currentPart = 1;
            boolean hasNextPart = true;
            boolean foundPart = true;
            for (int segment = initialSegment; hasNextPart; segment++, foundPart = false) {
              for (int part = currentPart; Files.exists(filePartPath); part++, filePartPath = Path
                .of(pathWithoutSegment, "seg" + segment, path + "_part" + String.format("%03d", part + 1))) {
                foundPart = true;
                currentPart++;
                InputStream partStream = Files.newInputStream(filePartPath);
                sequenceStream = new SequenceInputStream(sequenceStream, partStream);
              }
              if (!foundPart) {
                hasNextPart = false;
              }
            }
            return sequenceStream;
          } else {
            throw new IOException("Couldn't find file");
          }
        }
      } catch (IOException e) {
        throw new ModuleException()
          .withMessage(
            String.format("Could not open file at %s for reading.", container.getPath().resolve(Paths.get(path))))
          .withCause(e);
      }
    }

  }

  @Override
  public boolean isSimultaneousReadingSupported() {
    return true;
  }

  @Override
  public void finish(SIARDArchiveContainer container) throws ModuleException {
    if (container == mainContainer) {
      zipRead.finish(container);
    }
  }

  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    if (container == mainContainer) {
      zipRead.setup(container);
    } else if (mainContainer != null && mainContainer.getVersion() != null && container != null
      && container.getVersion() == null) {
      container.setVersion(mainContainer.getVersion());
    }
  }

  @Override
  public CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container) throws ModuleException {
    if (container == mainContainer) {
      return zipRead.getFilepathStream(container);
    } else {
      DirectoryStream<Path> nullableDirectoryStream = null;
      try {
        nullableDirectoryStream = Files.newDirectoryStream(container.getPath());
        final DirectoryStream<Path> directoryStream = nullableDirectoryStream;
        final Iterator<Path> iterator = directoryStream.iterator();
        return new CloseableIterable<String>() {
          @Override
          public void close() throws IOException {
            directoryStream.close();
          }

          @Override
          public Iterator<String> iterator() {
            return new Iterator<String>() {
              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public String next() {
                return iterator.next().toString();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException("remove() is not supported for this iterator");
              }
            };
          }
        };
      } catch (IOException e) {
        throw new ModuleException().withCause(e);
      } finally {
        if (nullableDirectoryStream != null) {
          try {
            nullableDirectoryStream.close();
          } catch (IOException e) {
            LOGGER.debug("Problem trying to close directory stream", e);
          }
        }
      }
    }
  }
}
