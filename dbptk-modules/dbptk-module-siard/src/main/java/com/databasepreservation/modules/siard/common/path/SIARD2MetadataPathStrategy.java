/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common.path;

import java.security.InvalidParameterException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2MetadataPathStrategy implements MetadataPathStrategy {
  // names for directories
  private static final String HEADER_DIR = "header";
  private static final String SCHEMA_RESOURCE_DIR = "schema";

  // names for files
  private static final String METADATA_FILENAME = "metadata";
  private static final String METADATA_RESOURCE_FILENAME_SIARD_20 = "siard2-0-metadata";
  private static final String METADATA_RESOURCE_FILENAME_SIARD_21 = "siard2-1-metadata";

  // extensions for files
  private static final String XML_EXTENSION = "xml";
  private static final String XSD_EXTENSION = "xsd";

  // control characters
  private static final String RESOURCE_FILE_SEPARATOR = "/";
  private static final String FILE_EXTENSION_SEPARATOR = ".";

  @Override
  public String getXmlFilePath(String filename) throws InvalidParameterException {

    if (filename.equals(METADATA_FILENAME)) {
      return new StringBuilder().append(HEADER_DIR).append(RESOURCE_FILE_SEPARATOR).append(METADATA_FILENAME)
        .append(FILE_EXTENSION_SEPARATOR).append(XML_EXTENSION).toString();
    } else {
      throw new InvalidParameterException(
        "Invalid metadata filename. Expected '" + METADATA_FILENAME + "', got '" + filename + "'.");
    }
  }

  @Override
  public String getXsdFilePath(String filename) throws InvalidParameterException {
    if (filename.equals(METADATA_FILENAME)) {
      return new StringBuilder().append(HEADER_DIR).append(RESOURCE_FILE_SEPARATOR).append(METADATA_FILENAME)
        .append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION).toString();
    } else {
      throw new InvalidParameterException(
        "Invalid metadata filename. Expected '" + METADATA_FILENAME + "', got '" + filename + "'.");
    }
  }

  @Override
  public String getXsdResourcePath(String filename) throws InvalidParameterException {
    if (filename.equals(METADATA_RESOURCE_FILENAME_SIARD_20) || filename.equals(METADATA_RESOURCE_FILENAME_SIARD_21)) {
      return new StringBuilder().append(RESOURCE_FILE_SEPARATOR).append(SCHEMA_RESOURCE_DIR)
        .append(RESOURCE_FILE_SEPARATOR).append(filename).append(FILE_EXTENSION_SEPARATOR).append(XSD_EXTENSION)
        .toString();
    } else {
      throw new InvalidParameterException("Invalid metadata filename. Expected '" + METADATA_RESOURCE_FILENAME_SIARD_20
        + "' or '" + METADATA_RESOURCE_FILENAME_SIARD_21 + "', got '" + filename + "'.");
    }
  }

}
