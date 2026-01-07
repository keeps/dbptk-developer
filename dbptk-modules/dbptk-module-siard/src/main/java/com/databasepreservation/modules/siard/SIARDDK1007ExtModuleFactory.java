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

import com.databasepreservation.modules.siard.in.input.SIARDDK1007ExtImportModule;
import com.databasepreservation.modules.siard.in.input.SIARDDKImportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDK1007ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK1007ExtModuleFactory extends SIARDDKModuleFactory {

  @Override
  String getModuleFactoryName() {
    return "siard-dk-1007-ext";
  }

  @Override
  SIARDDKImportModule createSIARDDKImportModuleInstance(Path path, String schemaName) {
    return new SIARDDK1007ExtImportModule(path, schemaName);
  }

  @Override
  SIARDDKExportModule createSIARDDKExportModuleInstance(Map<String, String> exportModuleArgs) {
    return new SIARDDK1007ExportModule(exportModuleArgs);
  }
}
