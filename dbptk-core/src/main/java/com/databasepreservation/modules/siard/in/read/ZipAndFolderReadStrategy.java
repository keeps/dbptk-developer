package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Read strategy that uses a ZipReadStrategy to read from the main SIARD archive container and a folder read strategy
 * to read from the auxiliary SIARD archive containers.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipAndFolderReadStrategy implements ReadStrategy {
        private final ZipReadStrategy zipRead;
        private final SIARDArchiveContainer mainContainer;

        public ZipAndFolderReadStrategy(SIARDArchiveContainer mainContainer) {
                zipRead = new ZipReadStrategy();
                this.mainContainer = mainContainer;
        }

        @Override public InputStream createInputStream(SIARDArchiveContainer container, String path)
          throws ModuleException {
                if (container == mainContainer) {
                        return zipRead.createInputStream(container, path);
                } else {
                        try {
                                return Files.newInputStream(container.getPath().resolve(Paths.get(path)));
                        } catch (IOException e) {
                                throw new ModuleException(String.format("Could not open file at %s for reading.",
                                  container.getPath().resolve(Paths.get(path))), e);
                        }
                }
        }

        @Override public boolean isSimultaneousReadingSupported() {
                return true;
        }

        @Override public void finish(SIARDArchiveContainer container) throws ModuleException {
                if (container == mainContainer) {
                        zipRead.finish(container);
                }
        }

        @Override public void setup(SIARDArchiveContainer container) throws ModuleException {
                if (container == mainContainer) {
                        zipRead.setup(container);
                }
        }

        @Override public CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container)
          throws ModuleException {
                if (container == mainContainer) {
                        return zipRead.getFilepathStream(container);
                } else {
                        try {
                                final DirectoryStream<Path> directoryStream = Files
                                  .newDirectoryStream(container.getPath());
                                final Iterator<Path> iterator = directoryStream.iterator();
                                return new CloseableIterable<String>() {
                                        @Override public void close() throws IOException {
                                                directoryStream.close();
                                        }

                                        @Override public Iterator<String> iterator() {
                                                return new Iterator<String>() {
                                                        @Override public boolean hasNext() {
                                                                return iterator.hasNext();
                                                        }

                                                        @Override public String next() {
                                                                return iterator.next().toString();
                                                        }

                                                        @Override public void remove() {
                                                                throw new UnsupportedOperationException(
                                                                  "remove() is not supported for this iterator");
                                                        }
                                                };
                                        }
                                };
                        } catch (IOException e) {
                                throw new ModuleException("", e);
                        }
                }
        }
}
