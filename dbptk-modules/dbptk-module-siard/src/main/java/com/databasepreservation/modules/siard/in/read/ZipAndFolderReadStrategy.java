/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.read;

import java.io.IOException;
import java.io.InputStream;
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
public class ZipAndFolderReadStrategy implements ReadStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipAndFolderReadStrategy.class);
  private final ZipReadStrategy zipRead;
  private final SIARDArchiveContainer mainContainer;

  public ZipAndFolderReadStrategy(SIARDArchiveContainer mainContainer) {
    zipRead = new ZipReadStrategy();
    this.mainContainer = mainContainer;
  }

  @Override
  public InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    if (container == mainContainer) {
      return zipRead.createInputStream(container, path);
    } else {
      try {
        return Files.newInputStream(container.getPath().resolve(Paths.get(path)));
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
