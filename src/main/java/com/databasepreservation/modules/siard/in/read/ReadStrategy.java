package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

import java.io.InputStream;
import java.util.List;

/**
 * Defines the behaviour for reading data
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ReadStrategy {
	InputStream createInputStream(SIARDArchiveContainer container, String path) throws ModuleException;

	/**
	 * @return true if the WriteStrategy supports reading from a new file before closing the previous one
	 */
	boolean isSimultaneousReadingSupported();

	/**
	 * Handles closing of the underlying structure used by this ReadStrategy object
	 * @throws ModuleException
	 */
	void finish(SIARDArchiveContainer container) throws ModuleException;

	/**
	 * Handles setting up the underlying structure used by this ReadStrategy object
	 * @throws ModuleException
	 */
	void setup(SIARDArchiveContainer container) throws ModuleException;

	/**
	 * @param container The container to list the files
	 * @param directory The directory (relative to container base path) to list the files
	 * @return List of paths for files contained in the specified directory. The paths returned are suitable to be used in createInputStream
	 * @throws ModuleException
	 */
	List<String> listFiles(SIARDArchiveContainer container, String directory) throws ModuleException;
}
