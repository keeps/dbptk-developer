package com.databasepreservation.modules.siard.common.path;

import java.security.InvalidParameterException;

/**
 * Defines an API to get the locations of the main XML metadata file and the
 * respective XML schema validator file. the locations should be returned as
 * relative paths as if the siard file was the root folder.
 *
 * Example: to refer the file at ".../database.siard/header/metadata.xml", the
 * value "header/metadata.xml" should be returned.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataPathStrategy {

  /**
   * Returns the path to the metedata XML-file with name filename
   */
  public String getXmlFilePath(String filename) throws InvalidParameterException;

  /**
   * Returns the path to the metadata XSD-file with name filename
   */
  public String getXsdFilePath(String filename) throws InvalidParameterException;
}
