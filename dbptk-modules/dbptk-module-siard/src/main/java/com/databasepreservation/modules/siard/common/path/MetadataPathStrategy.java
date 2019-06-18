/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common.path;

import java.io.IOException;
import java.security.InvalidParameterException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.read.CloseableIterable;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

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

  /**
   * 
   * @param filename
   * @return path of the relevant schema file in the resource folder
   * @throws InvalidParameterException
   */
  public String getXsdResourcePath(String filename) throws InvalidParameterException;

  class VersionIdentifier {
    private static String[] paths2_0 = new String[] {"header/version/2.0/", "header/version/2.0"};
    private static String[] paths2_1 = new String[] {"header/siardversion/2.1/", "header/siardversion/2.1",
      "header/version/2.1/", "header/version/2.1"};

    /**
     * Identifies the SIARD 2 minor version
     */
    public static SIARDConstants.SiardVersion getVersion(ReadStrategy readStrategy,
      SIARDArchiveContainer mainContainer) {
      try (CloseableIterable<String> pathIterator = readStrategy.getFilepathStream(mainContainer)) {
        for (String path : pathIterator) {
          for (String p : paths2_0) {
            if (p.equalsIgnoreCase(path)) {
              return SIARDConstants.SiardVersion.V2_0;
            }
          }
          for (String p : paths2_1) {
            if (p.equalsIgnoreCase(path)) {
              return SIARDConstants.SiardVersion.V2_1;
            }
          }
        }
      } catch (IOException | ModuleException e) {
        // ignore
      }
      return SIARDConstants.SiardVersion.V2_0;
    }
  }
}
