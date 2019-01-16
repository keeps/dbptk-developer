/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentExportStrategy {
  void openSchema(SchemaStructure schema) throws ModuleException;

  void closeSchema(SchemaStructure schema) throws ModuleException;

  void openTable(TableStructure table) throws ModuleException;

  void closeTable(TableStructure table) throws ModuleException;

  void tableRow(Row row) throws ModuleException;

  void setOnceReporter(Reporter reporter);
}
