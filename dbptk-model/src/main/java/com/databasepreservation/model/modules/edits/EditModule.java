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
import com.databasepreservation.model.modules.ExceptionNormalizer;
import com.databasepreservation.model.structure.DatabaseStructure;

import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface EditModule extends ExceptionNormalizer {

  /**
   * The reporter is set specifically for each module/filter
   *
   * @param reporter
   *          The reporter that should be used by this EditModule
   */
  void setOnceReporter(Reporter reporter);

  DatabaseStructure getMetadata() throws ModuleException;

  List<String> getXSD() throws ModuleException;
}
