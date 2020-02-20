/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.modules;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;

/**
 * @author Luis Faria <lfaria@keep.pt>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface DatabaseImportModule extends ExceptionNormalizer {
  /**
   * Import the database model.
   *
   * @param databaseExportModule
   *          The database model handler to be called when importing the database.
   * @return Return null unless this DatabaseImportModule also implements
   *         DatabaseExportModule
   * @throws UnknownTypeException
   *           a type used in the original database structure is unknown and
   *           cannot be mapped
   * @throws InvalidDataException
   *           the database data is not valid
   * @throws ModuleException
   *           generic module exception
   */
  DatabaseFilterModule migrateDatabaseTo(DatabaseFilterModule databaseExportModule) throws ModuleException;

  /**
   * Provide a reporter through which potential conversion problems should be
   * reported. This reporter should be provided only once for the export module
   * instance.
   *
   * @param reporter
   *          The initialized reporter instance.
   */
  void setOnceReporter(Reporter reporter);
}
