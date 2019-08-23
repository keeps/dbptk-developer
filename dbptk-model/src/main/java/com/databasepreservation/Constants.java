/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class Constants {
  public static final String VERSION_INFO_FILE = "dbptk-version.json";
  public static final String PROPERTY_KEY_HOME = "dbptk.home";
  public static final String LOGBACK_FILE_NAME = "logback_manual.xml";
  public static final String UNSPECIFIED_METADATA_VALUE = "unspecified";

  /* STYLE */
  public static final String SMALL_SPACE = "      ";
  public static final String MEDIUM_SPACE = "        ";
  public static final String SEPARATOR = "---";

  /* FILE HELPERS */
  public static final String RESOURCE_FILE_SEPARATOR = "/";
  public static final String FILE_EXTENSION_SEPARATOR = ".";

  /* CLI */
  public static final int CLI_LINE_WIDTH = 80;
  public static final String DBPTK_OPTION_MIGRATE = "migrate";
  public static final String DBPTK_OPTION_EDIT = "edit";
  public static final String DBPTK_OPTION_HELP = "help";
  public static final String DBPTK_OPTION_VALIDATE = "validate";
  public static final String DBPTK_OPTION_HELP_SMALL = "-h";

  /* SIARD FILE */
  public static final String SIARD_HEADER_FOLDER = "header";
  public static final String SIARD_CONTENT_FOLDER = "content";
  public static final String SIARD_VERSION_FOLDER = "siardversion";
  public static final String SIARD_METADATA_FILE = "metadata";
  public static final String SIARD_EXTENSION = ".siard";

  /* FILE EXTENSIONS */
  public static final String XSD_EXTENSION = ".xsd";
  public static final String XML_EXTENSION = ".xml";
  public static final String TXT_EXTENSION = ".txt";
  public static final String BIN_EXTENSION = ".bin";

  /* ADVANCED OR STRUCTURED DATA TYPES */
  public static final String UDT = "udt";
  public static final String DISTINCT = "distinct";

  /* XML NAMESPACES */
  public static final String NAMESPACE_FOR_METADATA = "metadata";
  public static final String NAMESPACE_FOR_TABLE = "table";

  /* VALIDATOR */
  public static final String DBPTK_VALIDATION_REPORTER_PREFIX = "dbptk-validation-reporter";
  public static final String SCHEMA = "schema";
  public static final String ROUTINE = "routine";
  public static final String VIEW = "view";
  public static final String USER = "user";
  public static final String TABLES = "tables";
  public static final String TABLE = "table";
  public static final String ROWS = "rows";
  public static final String COLUMN = "column";
  public static final String COLUMNS = "columns";
  public static final String PRIMARY_KEY = "primaryKey";
  public static final String FOREIGN_KEY = "foreignKey";
  public static final String CANDIDATE_KEY = "candidateKey";
  public static final String CHECK_CONSTRAINT = "checkConstraint";
  public static final String NAME = "name";
  public static final String FOLDER = "folder";
  public static final String LOB_FOLDER = "lobFolder";
  public static final String CONDITIONAL = "conditional";
  public static final String DESCRIPTION = "description";
  public static final String TYPE = "type";
  public static final String TYPE_NAME = "typeName";
  public static final String TYPE_ORIGINAL = "typeOriginal";
  public static final String ATTRIBUTE = "attribute";

  /* VALIDATOR COMPONENTS */
  public static final String COMPONENT_ZIP_CONSTRUCTION = "Construction of the SIARD archive file";
  public static final String COMPONENT_SIARD_STRUCTURE = "Structure of the SIARD archive file";
  public static final String COMPONENT_METADATA_AND_TABLE_DATA = "Correspondence between metadata and table data";
  public static final String COMPONENT_ADDITIONAL_CHECKS = "Additional Checks";
  public static final String COMPONENT_DATE_AND_TIMESTAMP_DATA = "Date and timestamp data cells";
  public static final String COMPONENT_REQUIREMENTS_FOR_TABLE_DATA = "Requirements for table data";
  public static final String COMPONENT_TABLE_DATA = "Table data";
  public static final String COMPONENT_TABLE_SCHEMA_DEFINITION = "Table schema definition";
  public static final String COMPONENT_END_TAG = "end";
}