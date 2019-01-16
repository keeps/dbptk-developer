/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ExampleImportModule implements DatabaseImportModule {
  private final String text;
  private final Boolean active;

  public ExampleImportModule(String text, Boolean active) {
    this.text = text;
    this.active = active;
  }

  @Override
  public void getDatabase(DatabaseExportModule databaseExportModule)
    throws ModuleException, UnknownTypeException, InvalidDataException {
    databaseExportModule.initDatabase();

    System.out.println("[ExampleImportModule] active = " + active);
    System.out.println("[ExampleImportModule] text = " + text);

    databaseExportModule.finishDatabase();
  }
}
