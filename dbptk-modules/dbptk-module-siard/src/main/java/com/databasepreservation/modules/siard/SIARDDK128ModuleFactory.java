/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import com.databasepreservation.modules.siard.in.input.SIARDDK128ImportModule;
import com.databasepreservation.modules.siard.in.input.SIARDDKImportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDK128ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK128ModuleFactory extends SIARDDKModuleFactory {

  @Override
  String getModuleFactoryName() {
    return "siard-dk-128";
  }

  @Override
  SIARDDKImportModule createSIARDDKImportModuleInstance(Path path, String schemaName) {
    return new SIARDDK128ImportModule(path, schemaName);
  }

  @Override
  SIARDDKExportModule createSIARDDKExportModuleInstance(Map<String, String> exportModuleArgs) {
    return new SIARDDK128ExportModule(exportModuleArgs);
  }
}