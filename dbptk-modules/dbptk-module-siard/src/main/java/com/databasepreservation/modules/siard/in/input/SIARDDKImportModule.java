package com.databasepreservation.modules.siard.in.input;

import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.content.ContentImportStrategy;
import com.databasepreservation.modules.siard.in.metadata.MetadataImportStrategy;
import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;
import com.databasepreservation.utils.MapUtils;

import java.nio.file.Path;
import java.util.Map;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public abstract class SIARDDKImportModule {
  protected final FolderReadStrategyMD5Sum readStrategy;
  protected final SIARDArchiveContainer mainContainer;
  protected final MetadataImportStrategy metadataStrategy;
  protected final ContentImportStrategy contentStrategy;
  protected final SIARDDKPathImportStrategy pathStrategy; // Adicionado ao construtor
  public final String paramImportAsSchema;
  private final String moduleName;

  public SIARDDKImportModule(String moduleName, Path siardPackage, String paramImportAsSchema,
    SIARDDKPathImportStrategy pathStrategy) {
    this.moduleName = moduleName;
    this.paramImportAsSchema = paramImportAsSchema;
    this.mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK,
      siardPackage.toAbsolutePath().normalize(), SIARDArchiveContainer.OutputContainerType.MAIN);
    this.readStrategy = new FolderReadStrategyMD5Sum(mainContainer);
    this.pathStrategy = pathStrategy; // Inicializa o pathStrategy no construtor

    this.metadataStrategy = createMetadataImportStrategy();
    this.contentStrategy = createContentImportStrategy();
  }

  protected abstract MetadataImportStrategy createMetadataImportStrategy();

  protected abstract ContentImportStrategy createContentImportStrategy();

  public DatabaseImportModule getDatabaseImportModule() {
    final Map<String, String> properties = MapUtils.buildMapFromObjects(getModuleFactoryParameterFolder(),
      mainContainer.getPath().normalize().toAbsolutePath().toString(), getModuleFactoryParameterAsSchema(),
      paramImportAsSchema);
    return new SIARDImportDefault(moduleName, contentStrategy, mainContainer, readStrategy, metadataStrategy,
      properties);
  }

  protected abstract String getModuleFactoryParameterFolder();

  protected abstract String getModuleFactoryParameterAsSchema();
}
