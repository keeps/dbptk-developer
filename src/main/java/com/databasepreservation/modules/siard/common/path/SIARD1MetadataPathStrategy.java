package com.databasepreservation.modules.siard.common.path;

import java.io.File;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1MetadataPathStrategy implements MetadataPathStrategy {
	// names for directories
	private static final String HEADER_DIR = "header";

	// names for files
	private static final String METADATA_FILENAME = "metadata";

	// extensions for files
	private static final String XML_EXTENSION = "xml";
	private static final String XSD_EXTENSION = "xsd";

	// control characters
	private static final String FILE_SEPARATOR = File.separator; // is "/" on Unix and "\\" on Windows
	private static final String FILE_EXTENSION_SEPARATOR = ".";

	@Override
	public String getMetadataXmlFilePath() {
		return new StringBuilder()
				.append(HEADER_DIR)
				.append(FILE_SEPARATOR)
				.append(METADATA_FILENAME)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XML_EXTENSION)
				.toString();
	}

	@Override
	public String getMetadataXsdFilePath() {
		return new StringBuilder()
				.append(HEADER_DIR)
				.append(FILE_SEPARATOR)
				.append(METADATA_FILENAME)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XSD_EXTENSION)
				.toString();
	}
}
