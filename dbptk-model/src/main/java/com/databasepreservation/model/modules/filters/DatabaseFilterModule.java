package com.databasepreservation.model.modules.filters;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;

/**
 * A Database Filter Module stands in between an import module, an export module
 * (that is not a filter), or other filters.
 *
 * Using getDatabase, the DatabaseImportModule obtains the database and passes
 * it to a DatabaseExportModule (passed via parameter). This is the most simple
 * scenario.
 *
 * Filters allow data to be analysed or changes to be made to the database while
 * it is traveling from the DatabaseImportModule to the DatabaseExportModule.
 * Filters are both import and export modules in terms of java interfaces.
 *
 * Example:
 *
 * <pre>
 * mySQLImportModule.getDatabase( // gets data from mysql, sends it to
 *                                // loggingFilter
 * 
 *   progressLoggingFilter.getDatabase( // gets data from mysql, outputs some
 *                                      // logging information without changing the
 *                                      // data (this is the filter action), sends
 *                                      // data to anonymizationFilter
 * 
 *     anonymizationFilter.getDatabase( // gets data from the loggingFilter,
 *                                      // anonymizes the database structure and
 *                                      // data, and sends it to the
 *                                      // siardExportModule
 * 
 *       siardExportModule // siard module receives the anonymized data from the
 *                         // anonymization filter and saves it to siard. Since
 *                         // filters act as import modules (which send data to
 *                         // export modules) the siard export module (or any other
 *                         // export module) can export data coming from a filter.
 *     )))
 * </pre>
 *
 * In the original code, which did not support filters, the conversion would go
 * like
 * 
 * <pre>
 * mySQLImportModule -&gt; siardExportModule
 * </pre>
 * 
 * So to have the same functionality as described above, one of the modules (or
 * both) would have to do the progress logging, and one of them would need to be
 * responsible for anonymizing the database.
 *
 * DatabaseFilterModules provide a way to use the Filter Design Pattern to
 * modify the database seamlessly as it is "streamed" from the import module to
 * the export module. And in this case the functionality above would be
 * implemented as:
 *
 * <pre>
 * mySQLImportModule -&gt; progressLoggingFilter -&gt; anonymizationFilter -&gt; siardExportModule
 * </pre>
 *
 * This flow of information is the same as described above in the form of
 * 'getDatabase' chained calls.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface DatabaseFilterModule extends DatabaseImportModule, DatabaseExportModule {
  /**
   * The reporter is set specifically for each module/filter, so this call does
   * not need to be chained to the next DatabaseFilterModule
   * 
   * @param reporter
   *          The reporter that should be used by this DatabaseFilterModule
   */
  @Override
  void setOnceReporter(Reporter reporter);

  /**
   * Import the database model.
   *
   * @param databaseExportModule
   *          The database model handler to be called when importing the database.
   * @return Return itself, to allow chaining multiple getDatabase methods
   * @throws ModuleException
   *           generic module exception
   */
  @Override
  DatabaseExportModule migrateDatabaseTo(DatabaseExportModule databaseExportModule) throws ModuleException;
}
