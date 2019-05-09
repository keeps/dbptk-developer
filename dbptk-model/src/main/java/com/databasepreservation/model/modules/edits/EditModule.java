/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.edits;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseImportModule;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface EditModule extends DatabaseImportModule {
  /**
   * The reporter is set specifically for each module/filter
   *
   * @param reporter
   *          The reporter that should be used by this DatabaseFilterModule
   */
  @Override
  void setOnceReporter(Reporter reporter);

  /**
   * Import the database model.
   *
   *
   * @return Return itself, to allow chaining multiple getDatabase methods
   * @throws ModuleException
   *           generic module exception
   */
  DatabaseImportModule edit() throws ModuleException;
}
