package dk.magenta.siarddk;

import java.io.File;

import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;

public class SIARDDKMetadataPathStrategy implements MetadataPathStrategy {

	// Extensions for files
	private static final String XML_EXTENSION = "xml";
	private static final String XSD_EXTENSION = "xsd";
	
	// control characters
	private static final String FILE_SEPARATOR = File.separator; // is "/" on Unix and "\\" on Windows
	private static final String FILE_EXTENSION_SEPARATOR = ".";

	// Names for directories
	private static final String INDICES_DIR = "Indices";
	private static final String SCHEMA_DIR = "Schemas" + FILE_SEPARATOR + "standard";

	
	@Override
	public String getXmlFilePath(String filename) {
		return new StringBuilder()
			.append(INDICES_DIR)
			.append(FILE_SEPARATOR)
			.append(filename)
			.append(FILE_EXTENSION_SEPARATOR)
			.append(XML_EXTENSION)
			.toString();
	}

	@Override
	public String getXsdFilePath(String filename) {
		return new StringBuilder()
		.append(SCHEMA_DIR)
		.append(FILE_SEPARATOR)
		.append(filename)
		.append(FILE_EXTENSION_SEPARATOR)
		.append(XSD_EXTENSION)
		.toString();
	}

}
