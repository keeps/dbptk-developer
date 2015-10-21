package com.databasepreservation.modules.siard.constants;

import java.io.File;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKConstants {

  // System dependent file seperator etc. ("/" on Linux and "\" on Windows)
  public static final String FILE_SEPARATOR = File.separator;
  public static final String FILE_EXTENSION_SEPARATOR = ".";

  // File extensions
  public static final String XML_EXTENSION = "xml";
  public static final String XSD_EXTENSION = "xsd";

  // Name of the context documentation folder within the archive
  public static final String CONTEXT_DOCUMENTATION_RELATIVE_PATH = "ContextDocumentation";

  // Path to schemas in the /src/main/resources folder
  public static final String SCHEMA_RESOURCE_DIR = "schema";

  // JAXB context for tableIndex and fileIndex
  public static final String JAXB_CONTEXT_TABLEINDEX = "dk.sa.xmlns.diark._1_0.tableindex";
  public static final String JAXB_CONTEXT_FILEINDEX = "dk.sa.xmlns.diark._1_0.fileindex";

  // Key for context documentation folder (given on command line)
  public static final String CONTEXT_DOCUMENTATION_FOLDER = "contextDocumentationFolder";

  // Keys used in the metadata contexts
  public static final String CONTEXT_DOCUMENTATION_INDEX = "contextDocumentationIndex";
  public static final String ARCHIVE_INDEX = "archiveIndex";
  public static final String TABLE_INDEX = "tableIndex";
  public static final String FILE_INDEX = "fileIndex";
  public static final String DOC_INDEX = "docIndex";
  public static final String XML_SCHEMA = "XMLSchema";

  // Maximum number of files that can be stored in folder
  public static final int MAX_NUMBER_OF_FILES = 10000;
}
