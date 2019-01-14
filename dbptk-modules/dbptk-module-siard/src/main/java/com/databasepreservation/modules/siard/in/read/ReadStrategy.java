package com.databasepreservation.modules.siard.in.read;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * Defines the behaviour for reading data
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ReadStrategy {
  InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException;

  /**
   * @return true if the ReadStrategy supports reading from a new file before
   *         closing the previous one
   */
  boolean isSimultaneousReadingSupported();

  /**
   * Handles closing of the underlying structure used by this ReadStrategy object
   * for this container
   *
   * @throws ModuleException
   */
  void finish(SIARDArchiveContainer container) throws ModuleException;

  /**
   * Handles setting up the underlying structure used by this ReadStrategy object
   * to use this container
   *
   * @throws ModuleException
   */
  void setup(SIARDArchiveContainer container) throws ModuleException;

  /**
   * @param container
   *          The container to list the files
   * @return Iterable of paths for files contained in the specified directory. The
   *         paths are suitable to be used in createInputStream. After use it
   *         should be closed.
   * @throws ModuleException
   */
  CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container) throws ModuleException;
}
