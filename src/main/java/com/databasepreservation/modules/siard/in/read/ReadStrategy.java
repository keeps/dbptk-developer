package com.databasepreservation.modules.siard.in.read;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.out.write.OutputContainer;

import java.io.InputStream;
import java.util.List;

/**
 * Defines the behaviour for reading data
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ReadStrategy {
	InputStream createInputStream(OutputContainer container, String path) throws ModuleException;

	/**
	 * @return true if the WriteStrategy supports reading from a new file before closing the previous one
	 */
	boolean isSimultaneousReadingSupported();

	/**
	 * Handles closing of the underlying structure used by this ReadStrategy object
	 * @throws ModuleException
	 */
	void finish(OutputContainer container) throws ModuleException;

	/**
	 * Handles setting up the underlying structure used by this ReadStrategy object
	 * @throws ModuleException
	 */
	void setup(OutputContainer container) throws ModuleException;

	List<String> listFiles(OutputContainer container, String directory) throws ModuleException;
}
