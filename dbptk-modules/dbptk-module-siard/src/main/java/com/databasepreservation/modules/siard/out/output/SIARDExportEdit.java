/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.edits.EditExportModule;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDExportEdit implements EditExportModule {

  public SIARDExportEdit() {}

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }
}
