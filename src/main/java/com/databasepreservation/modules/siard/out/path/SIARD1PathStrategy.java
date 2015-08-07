package com.databasepreservation.modules.siard.out.path;

import java.io.File;

/**
 * Defines a SIARD 1.0 implementation to get paths to folders and files
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1PathStrategy implements PathStrategy{
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

	@Override
	public String getClobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(FILE_SEPARATOR)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(FILE_SEPARATOR)
				.append(LOB_DIR)
				.append(columnIndex)
				.append(FILE_SEPARATOR)
				.append(LOB_FILENAME)
				.append(rowIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(CLOB_EXTENSION)
				.toString();
	}

	@Override
	public String getBlobFilePath(int schemaIndex, int tableIndex, int columnIndex, int rowIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(FILE_SEPARATOR)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(FILE_SEPARATOR)
				.append(LOB_DIR)
				.append(columnIndex)
				.append(FILE_SEPARATOR)
				.append(LOB_FILENAME)
				.append(rowIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(BLOB_EXTENSION)
				.toString();
	}

	@Override
	public String getSchemaFolderName(int schemaIndex) {
		return new StringBuilder()
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.toString();
	}

	@Override
	public String getTableFolderName(int tableIndex) {
		return new StringBuilder()
				.append(TABLE_DIR)
				.append(tableIndex)
				.toString();
	}

	@Override
	public String getTableXsdFilePath(int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(FILE_SEPARATOR)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XSD_EXTENSION)
				.toString();
	}

	@Override
	public String getTableXmlFilePath(int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(CONTENT_DIR)
				.append(FILE_SEPARATOR)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_DIR)
				.append(tableIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XML_EXTENSION)
				.toString();
	}

	@Override
	public String getTableXsdNamespace(String base, int schemaIndex, int tableIndex) {
		return new StringBuilder()
				.append(base)
				.append(SCHEMA_DIR)
				.append(schemaIndex)
				.append(FILE_SEPARATOR)
				.append(TABLE_FILENAME)
				.append(tableIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XSD_EXTENSION)
				.toString();
	}

	@Override
	public String getTableXsdFileName(int tableIndex) {
		return new StringBuilder()
				.append(tableIndex)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XML_EXTENSION)
				.toString();
	}

}
