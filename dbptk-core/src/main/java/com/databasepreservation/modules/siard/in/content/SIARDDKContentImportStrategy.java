package com.databasepreservation.modules.siard.in.content;

import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

public class SIARDDKContentImportStrategy extends DefaultHandler implements ContentImportStrategy {

  protected final ReadStrategy readStrategy;
  protected final ContentPathImportStrategy contentPathStrategy;

  // TODO: Implement this!
  /**
   * @author Thomas Kristensen <tk@bithuset.dk>
   *
   */
  public SIARDDKContentImportStrategy(ReadStrategy readStrategy, ContentPathImportStrategy contentPathStrategy) {
    this.readStrategy = readStrategy;
    this.contentPathStrategy = contentPathStrategy;
  }

  @Override
  public void importContent(DatabaseExportModule handler, SIARDArchiveContainer container,
    DatabaseStructure databaseStructure) throws ModuleException {
    // TODO Auto-generated method stub

  }

}
