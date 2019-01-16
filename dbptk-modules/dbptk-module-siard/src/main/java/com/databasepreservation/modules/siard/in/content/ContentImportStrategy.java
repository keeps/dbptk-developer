/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.content;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentImportStrategy {
  void importContent(DatabaseExportModule handler, SIARDArchiveContainer container, DatabaseStructure databaseStructure,
    ModuleSettings moduleSettings) throws ModuleException;
}
