package dk.magenta.siarddk;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.out.output.SIARDExportDefault;

public class SIARDDKDatabaseExportModule extends SIARDExportDefault {

  // Could we add getters and setters for this in the super class?

  // private SIARDArchiveContainer mainContainer;
  // private WriteStrategy writeStrategy;

  private SIARDDKExportModule siarddkExportModule;

  // public SIARDDKDatabaseExportModule(ContentExportStrategy
  // contentExportStrategy, SIARDArchiveContainer mainContainer,
  // WriteStrategy writeStrategy, MetadataExportStrategy metadataExportStrategy)
  // {

  public SIARDDKDatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    // super(contentExportStrategy, mainContainer, writeStrategy,
    // metadataExportStrategy);

    super(siarddkExportModule.getContentExportStrategy(), siarddkExportModule.getMainContainer(), siarddkExportModule
      .getWriteStrategy(), siarddkExportModule.getMetadataExportStrategy());

    this.siarddkExportModule = siarddkExportModule;
    // this.mainContainer = mainContainer;
    // this.writeStrategy = writeStrategy;
  }

  @Override
  public void finishDatabase() throws ModuleException {
    super.finishDatabase();

    // Write ContextDocumentation to archive

    ContextDocumentationWriter contextDocumentationWriter = new ContextDocumentationWriter(
      siarddkExportModule.getMainContainer(), siarddkExportModule.getWriteStrategy(),
      siarddkExportModule.getFileIndexFileStrategy(), siarddkExportModule.getExportModuleArgs());

    contextDocumentationWriter.writeContextDocumentation();

  }
}
