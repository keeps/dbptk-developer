package com.databasepreservation.modules.siard.out.metadata;

import java.io.IOException;
import java.io.OutputStream;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.adapters.SIARDDKAdapter;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.output.SIARDDKExportModule;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK128MetadataExportStrategy extends SIARDDKMetadataExportStrategy {

  public SIARDDK128MetadataExportStrategy(SIARDDKExportModule siarddkExportModule, SIARDDKAdapter siarddkAdapter) {
    super(siarddkExportModule, siarddkAdapter);
  }

  @Override
  public void writeMetadataXML(DatabaseStructure dbStructure, SIARDArchiveContainer outputContainer,
    WriteStrategy writeStrategy) throws ModuleException {
    // TO-DO: Refactor this into one method in class that can be used by
    // SIARDDKDatabaseExportModule also

    // Generate tableIndex.xml

    try {
      IndexFileStrategy tableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(lobsTracker, siarddkAdapter);
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.TABLE_INDEX);
      OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

      siardMarshaller.marshal("com.databasepreservation.modules.siard.bindings.siard_dk_128",
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.TABLE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd", writer,
        tableIndexFileStrategy.generateXML(dbStructure));

      writer.close();

      SIARDDKFileIndexFileStrategy.addFile(path);

    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing tableIndex.xml to the archive.").withCause(e);
    }

    // Generate archiveIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.ARCHIVE_INDEX) != null) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.ARCHIVE_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy archiveIndexFileStrategy = new CommandLineIndexFileStrategy(SIARDDKConstants.ARCHIVE_INDEX,
          exportModuleArgs, writer, metadataPathStrategy);
        archiveIndexFileStrategy.generateXML(null);
        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing archiveIndex.xml to the archive").withCause(e);
      }
    }

    // Generate contextDocumentationIndex.xml

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX) != null) {
      try {

        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);
        IndexFileStrategy contextDocumentationIndexFileStrategy = new CommandLineIndexFileStrategy(
          SIARDDKConstants.CONTEXT_DOCUMENTATION_INDEX, exportModuleArgs, writer, metadataPathStrategy);
        contextDocumentationIndexFileStrategy.generateXML(null);
        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing contextDocumentationIndex.xml to the archive")
          .withCause(e);
      }
    }

    if (lobsTracker.getLOBsCount() > 0) {
      try {
        String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX);
        OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(outputContainer, path, writeStrategy);

        siardMarshaller.marshal(SIARDDKConstants.JAXB_CONTEXT_DOCINDEX,
          metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.DOC_INDEX),
          "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/docIndex.xsd", writer,
          SIARDDKDocIndexFileStrategy.generateXML(dbStructure));

        writer.close();

        SIARDDKFileIndexFileStrategy.addFile(path);

      } catch (IOException e) {
        throw new ModuleException().withMessage("Error writing docIndex.xml to the archive.").withCause(e);
      }
    }

    createLocalSharedFolder(outputContainer);
  }
}
