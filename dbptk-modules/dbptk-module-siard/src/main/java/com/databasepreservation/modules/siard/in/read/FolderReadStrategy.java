package com.databasepreservation.modules.siard.in.read;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class FolderReadStrategy implements ReadStrategy {

  protected final ZipAndFolderReadStrategy zipAndFolderReadStrategy;

  public FolderReadStrategy(SIARDArchiveContainer mainContainer) {
    // Adapter Strategy:
    // Create a ZipAndFolderReadStrategy with null as mainContainer reference to
    // get folder-read-strategy behavior out of the ZipAndFolderReadStrategy.
    zipAndFolderReadStrategy = new ZipAndFolderReadStrategy(null);
  }

  @Override
  public InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    return zipAndFolderReadStrategy.createInputStream(container, path);
  }

  @Override
  public boolean isSimultaneousReadingSupported() {
    return zipAndFolderReadStrategy.isSimultaneousReadingSupported();
  }

  @Override
  public void finish(SIARDArchiveContainer container) throws ModuleException {
    zipAndFolderReadStrategy.finish(container);

  }

  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    zipAndFolderReadStrategy.setup(container);

  }

  @Override
  public CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container) throws ModuleException {
    return zipAndFolderReadStrategy.getFilepathStream(container);
  }
}
