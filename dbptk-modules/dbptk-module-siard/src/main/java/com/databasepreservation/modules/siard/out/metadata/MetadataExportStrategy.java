/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataExportStrategy {
  void writeMetadataXML(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
    WriteStrategy writeStrategy) throws ModuleException;

  void writeMetadataXSD(DatabaseStructure databaseStructure, SIARDArchiveContainer container,
    WriteStrategy writeStrategy) throws ModuleException;

  void setOnceReporter(Reporter reporter);
}
