/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.update;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD1MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARD2MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDK1007MetadataPathStrategy;
import com.databasepreservation.modules.siard.common.path.SIARDDK128MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD1MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD20MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARD21MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK1007MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.SIARDDK128MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.ResourceFileIndexInputStreamStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD1ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARD2ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK1007PathImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDK128PathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipReadStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD1MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD20MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARD21MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD1ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.update.MetadataUpdateStrategy;
import com.databasepreservation.modules.siard.out.update.UpdateStrategy;
import com.databasepreservation.utils.ModuleConfigurationUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDDKEditModule implements EditModule {
  private final ReadStrategy readStrategy;
  private final SIARDArchiveContainer mainContainer;
  private MetadataImportStrategy metadataImportStrategy;

  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDKEditModule.class);

  private static final String METADATA_FILENAME = "metadata";

  /**
   * Constructor used to initialize required objects to get an edit import module
   * for SIARD 2 (all minor versions)
   *
   * @param siardPackagePath
   *          Path to the main SIARD file (file with extension .siard)
   */
  public SIARDDKEditModule(Path siardPackagePath) {
    Path siardPackageNormalizedPath = siardPackagePath.toAbsolutePath().normalize();
    String paramImportAsSchema = "public";

    if (Files.exists(Paths.get(siardPackagePath + SIARDDKConstants.SIARDDK_128_RESEARCH_INDEX_PATH))) {
      mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK_128, siardPackageNormalizedPath,
        SIARDArchiveContainer.OutputContainerType.MAIN);
      readStrategy = new FolderReadStrategyMD5Sum(mainContainer);

      MetadataPathStrategy metadataPathStrategy = new SIARDDK128MetadataPathStrategy();
      SIARDDK128PathImportStrategy pathStrategy = new SIARDDK128PathImportStrategy(mainContainer, readStrategy,
        metadataPathStrategy, paramImportAsSchema, new ResourceFileIndexInputStreamStrategy());

      metadataImportStrategy = new SIARDDK128MetadataImportStrategy(pathStrategy, paramImportAsSchema);
    } else {
      mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK_1007, siardPackageNormalizedPath,
        SIARDArchiveContainer.OutputContainerType.MAIN);
      readStrategy = new FolderReadStrategyMD5Sum(mainContainer);

      MetadataPathStrategy metadataPathStrategy = new SIARDDK1007MetadataPathStrategy();
      SIARDDK1007PathImportStrategy pathStrategy = new SIARDDK1007PathImportStrategy(mainContainer, readStrategy,
        metadataPathStrategy, paramImportAsSchema, new ResourceFileIndexInputStreamStrategy());

      metadataImportStrategy = new SIARDDK1007MetadataImportStrategy(pathStrategy, paramImportAsSchema);
    }
  }

  /**
   * Gets a <code>DatabaseStructure</code> with all the metadata imported from the
   * SIARD archive.
   *
   * @return A <code>DatabaseStructure</code>
   * @throws NullPointerException
   *          If the SIARD archive version were not 2.0 or 2.1
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public DatabaseStructure getMetadata() throws ModuleException {
    ModuleConfiguration moduleConfiguration = ModuleConfigurationUtils.getDefaultModuleConfiguration();

    LOGGER.info("Importing SIARD version {}", mainContainer.getVersion().getDisplayName());
    DatabaseStructure dbStructure;

    try {
      metadataImportStrategy.loadMetadata(readStrategy, mainContainer, moduleConfiguration);

      dbStructure = metadataImportStrategy.getDatabaseStructure();
    } catch (NullPointerException e) {
      throw new ModuleException().withMessage("Metadata editing only supports SIARD version 1, 2.0 and 2.1").withCause(e);
    } finally {
      readStrategy.finish(mainContainer);
    }
    return dbStructure;
  }

  @Override
  public String getSIARDVersion() {
    return mainContainer.getVersion().getDisplayName();
  }

  /**
   * @param dbStructure The {@link DatabaseStructure} with the updated values.
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public void updateMetadata(DatabaseStructure dbStructure) throws ModuleException {
    throw new ModuleException().withMessage("Metadata editing is not supported for SIARD version DK");
  }


  /**
   * @return A list of <code>SIARDDatabaseMetadata</code>
   * @throws ModuleException
   *          Generic module exception
   */
  @Override
  public List<SIARDDatabaseMetadata> getDescriptiveSIARDMetadataKeys() throws ModuleException {
    throw new ModuleException().withMessage("Metadata editing is not supported for SIARD version DK");
  }

  @Override
  public List<SIARDDatabaseMetadata> getDatabaseMetadataKeys() throws ModuleException {
    throw new ModuleException().withMessage("Metadata editing is not supported for SIARD version DK");
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
    metadataImportStrategy.setOnceReporter(reporter);
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }
}
