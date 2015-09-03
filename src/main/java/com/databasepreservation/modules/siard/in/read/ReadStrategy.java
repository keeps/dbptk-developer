package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * Defines the behaviour for reading data
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ReadStrategy {
        InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException;

        /**
         * @return true if the ReadStrategy supports reading from a new file before closing the previous one
         */
        boolean isSimultaneousReadingSupported();

        /**
         * Handles closing of the underlying structure used by this ReadStrategy object
         *
         * @throws ModuleException
         */
        void finish(SIARDArchiveContainer container) throws ModuleException;

        /**
         * Handles setting up the underlying structure used by this ReadStrategy object
         *
         * @throws ModuleException
         */
        void setup(SIARDArchiveContainer container) throws ModuleException;

        /**
         * @param container The container to list the files
         * @param parent    The directory (relative to container base path) which files should be listed. If null, the list will contain all file paths
         * @return Iterable of paths for files contained in the specified directory. The paths are suitable to be used in createInputStream. After use it should be closed.
         * @throws ModuleException
         */
        CloseableIterable<String> getFilepathStream(SIARDArchiveContainer container, Path parent)
          throws ModuleException;
}
