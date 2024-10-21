/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import java.nio.file.Path;
import java.util.Map;

import com.databasepreservation.modules.siard.in.input.SIARDDK1007ImportModule;
import com.databasepreservation.modules.siard.in.input.SIARDDKImportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;

import com.databasepreservation.modules.siard.out.output.SIARDDK1007ExportModule;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK1007ModuleFactory extends SIARDDKModuleFactory {

  @Override
  String getModuleFactoryName() {
    return "siard-dk-1007";
  }

  @Override
  SIARDDKImportModule createSIARDDKImportModuleInstance(Path path, String schemaName) {
    return new SIARDDK1007ImportModule(path, schemaName);
  }

  @Override
  SIARDDKExportModule createSIARDDKExportModuleInstance(Map<String, String> exportModuleArgs) {
    return new SIARDDK1007ExportModule(exportModuleArgs);
  }
}
