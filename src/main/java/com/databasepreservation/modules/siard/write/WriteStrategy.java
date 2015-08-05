package com.databasepreservation.modules.siard.write;

import java.io.*;

import com.databasepreservation.model.exception.ModuleException;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface WriteStrategy {
	/**
	 * Creates a stream through which data can be written to the output format
	 * @param container The container where the data will be written
	 * @param path The path (relative to the container) to the file where the data from the stream should be written to
	 * @return
	 *     an OutputStream that is able to write to the specified location in a way specific to the WriteStrategy,
	 *     this stream should be closed after use by calling the close() method
	 */
	OutputStream createOutputStream(OutputContainer container, String path) throws ModuleException;

	/**
	 * @return true if the WriteStrategy supports writing a to a new file before closing the previous one
	 */
	boolean supportsSimultaneousWriting();

	/**
	 * Handles closing of the underlying structure used by this WriteStrategy object
	 * @throws ModuleException
	 */
	void finish(OutputContainer container) throws ModuleException;

	/**
	 * Handles setting up the underlying structure used by this WriteStrategy object
	 * @throws ModuleException
	 */
	void setup(OutputContainer container) throws ModuleException;
}
