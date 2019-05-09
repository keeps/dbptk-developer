/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;

import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEditImport implements EditModule {

  private Reporter reporter;
  private Path SIARDPackage;

  private DatabaseStructure databaseStructure;

  public SIARDEditImport(Path SIARDPackage) {
    this.SIARDPackage = SIARDPackage;
  }

  @Override
  public DatabaseExportModule migrateDatabaseTo(DatabaseExportModule databaseExportModule) throws ModuleException {
    return null;
  }

  @Override
  public void setOnceReporter(Reporter reporter) { this.reporter = reporter; }

  @Override
  public DatabaseImportModule edit() throws ModuleException {
    return new SIARD2ImportModule(SIARDPackage).getDatabaseImportModule();
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }
}
