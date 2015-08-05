package com.databasepreservation.modules.siard.path;

/**
 * Defines a SIARD 1.0 implementation to get paths to folders and files
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PathStrategySIARD1 implements PathStrategy{
	// names for directories
	private static final String HEADER_DIR = "header";
	private static final String CONTENT_DIR = "content";
	private static final String METADATA_DIR = "metadata";
	private static final String SCHEMA_DIR = "schema";
	private static final String TABLE_DIR = "table";
	private static final String LOB_DIR = "lob";

	// names for files
	private static final String METADATA_FILENAME = "metadata";
	private static final String SCHEMA_FILENAME = "schema";
	private static final String TABLE_FILENAME = "table";
	private static final String LOB_FILENAME = "record";

	// extensions for files
	private static final String XML_EXTENSION = "xml";
	private static final String XSD_EXTENSION = "xsd";
	private static final String CLOB_EXTENSION = "txt";
	private static final String BLOB_EXTENSION = "bin";

	// control characters
	private static final String SLASH = "/";
	private static final String DOT = ".";

	@Override
	public String metadataXmlFile() {
		return new StringBuilder()
				.append(HEADER_DIR)
				.append(SLASH)
				.append(METADATA_FILENAME)
				.append(DOT)
				.append(XML_EXTENSION)
				.toString();
	}

	@Override
	public String metadataXsdFile() {
		return new StringBuilder()
				.append(HEADER_DIR)
				.append(SLASH)
				.append(METADATA_FILENAME)
				.append(DOT)
				.append(XSD_EXTENSION)
				.toString();
	}

	@Override
	public String clobFile(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(SLASH)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(SLASH)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(SLASH)
				.append(LOB_DIR)
				.append(columnIndex)
				.append(SLASH)
				.append(LOB_FILENAME)
				.append(rowIndex)
				.append(DOT)
				.append(CLOB_EXTENSION)
				.toString();
	}

	@Override
	public String blobFile(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(SLASH)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(SLASH)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(SLASH)
				.append(LOB_DIR)
				.append(columnIndex)
				.append(SLASH)
				.append(LOB_FILENAME)
				.append(rowIndex)
				.append(DOT)
				.append(BLOB_EXTENSION)
				.toString();
	}

	@Override
	public String schemaFolderName(int schemaIndex) {
		return new StringBuilder()
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.toString();
	}

	@Override
	public String tableFolderName(int tableIndex) {
		return new StringBuilder()
				.append(TABLE_DIR)
				.append(tableIndex)
				.toString();
	}

	@Override
	public String tableXsdFile(int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(SLASH)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(SLASH)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(SLASH)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(DOT)
				.append(XSD_EXTENSION)
				.toString();
	}

	@Override
	public String tableXmlFile(int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(SLASH)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(SLASH)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(SLASH)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(DOT)
				.append(XML_EXTENSION)
				.toString();
	}

	@Override
	public String tableXsdNamespace(String base, int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(base)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(SLASH)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(DOT)
				.append(XSD_EXTENSION)
				.toString();
	}

	@Override
	public String tableXsdName(int tableIndex) {
		return new StringBuilder()
				.append(tableIndex)
				.append(DOT)
				.append(XML_EXTENSION)
				.toString();
	}

}
