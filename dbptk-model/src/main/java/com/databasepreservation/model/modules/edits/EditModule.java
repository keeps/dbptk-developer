/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.edits;

import java.util.List;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.databasepreservation.model.modules.ExceptionNormalizer;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface EditModule extends ExceptionNormalizer {

  /**
   * The reporter is set specifically for each module
   *
   * @param reporter
   *          The reporter that should be used by this EditModule
   */
  void setOnceReporter(Reporter reporter);

  /**
   * Gets a <code>DatabaseStructure</code> with all the metadata imported from the
   * SIARD archive.
   *
   * @return A <code>DatabaseStructure</code>
   * @throws ModuleException
   *           Generic module exception
   */
  DatabaseStructure getMetadata() throws ModuleException;

  /**
   * Gets the version of the SIARD file.
   *
   * @return The SIARD version
   * @throws ModuleException
   *           Generic module exception
   */
  String getSIARDVersion();

  /**
   * Returns a list of <code>SIARDDatabaseMetadata</code> with the descriptive metadata.
   *
   * @return A list of <code>SIARDDatabaseMetadata</code>
   * @throws ModuleException
   *          Generic module exception
   */
  List<SIARDDatabaseMetadata> getDescriptiveSIARDMetadataKeys() throws ModuleException;

  /**
   * Returns a list of <code>SIARDDatabaseMetadata</code> with the database metadata.
   *
   * @return A list of <code>SIARDDatabaseMetadata</code>
   * @throws ModuleException
   *          Generic module exception
   */
  List<SIARDDatabaseMetadata> getDatabaseMetadataKeys() throws ModuleException;

  /**
   * Updates the SIARD archive from the <code>DatabaseStructure</code> with the
   * new values.
   *
   * @param dbStructure
   *          The {@link DatabaseStructure} with the updated values.
   * @throws ModuleException
   *          Generic module exception
   */
  void updateMetadata(DatabaseStructure dbStructure) throws ModuleException;
}
