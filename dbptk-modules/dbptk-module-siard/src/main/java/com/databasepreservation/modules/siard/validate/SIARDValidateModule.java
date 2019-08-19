package com.databasepreservation.modules.siard.validate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.modules.siard.validate.TableData.TableDataValidator;
import com.databasepreservation.modules.siard.validate.TableData.TableSchemaDefinitionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.modules.siard.validate.FormatStructure.MetadataAndTableDataValidator;
import com.databasepreservation.modules.siard.validate.FormatStructure.SIARDStructureValidator;
import com.databasepreservation.modules.siard.validate.FormatStructure.ZipConstructionValidator;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);
  private Reporter reporter;

  private final Path SIARDPackageNormalizedPath;
  private ValidationReporter validationReporter;
  private final List<String> allowedUDTs;

  /**
   * Constructor used to initialize required objects to get an validate module
   * for SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path SIARDPackagePath, Path validationReporterPath) {
    SIARDPackageNormalizedPath = SIARDPackagePath.toAbsolutePath().normalize();
    validationReporter = new ValidationReporter(validationReporterPath.toAbsolutePath().normalize(), SIARDPackageNormalizedPath);
    allowedUDTs = Collections.emptyList();
  }

  /**
   * Constructor used to initialize required objects to get an validate module
   * for SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path SIARDPackagePath, Path validationReporterPath, Path allowedUDTs) {
    SIARDPackageNormalizedPath = SIARDPackagePath.toAbsolutePath().normalize();
    validationReporter = new ValidationReporter(validationReporterPath.toAbsolutePath().normalize(), SIARDPackageNormalizedPath);
    this.allowedUDTs = parseAllowUDTs(allowedUDTs);
  }

  /**
   * The reporter is set specifically for each module
   *
   * @param reporter The reporter that should be used by this ValidateModule
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  /**
   * @throws ModuleException Generic module exception
   */
  @Override
  public void validate() throws ModuleException {
    final ZipConstructionValidator zipConstructionValidation = ZipConstructionValidator.newInstance();
    zipConstructionValidation.setSIARDPackagePath(SIARDPackageNormalizedPath);
    zipConstructionValidation.setReporter(reporter);
    zipConstructionValidation.setValidationReporter(validationReporter);
    zipConstructionValidation.validate();

    final SIARDStructureValidator siardStructureValidator = SIARDStructureValidator.newInstance();
    siardStructureValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    siardStructureValidator.setReporter(reporter);
    siardStructureValidator.setValidationReporter(validationReporter);
    siardStructureValidator.validate();

    final MetadataAndTableDataValidator metadataAndTableDataValidator = MetadataAndTableDataValidator.newInstance();
    metadataAndTableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataAndTableDataValidator.setReporter(reporter);
    metadataAndTableDataValidator.setValidationReporter(validationReporter);
    metadataAndTableDataValidator.setAllowUDTs(allowedUDTs);
    metadataAndTableDataValidator.validate();

    final TableDataValidator tableDataValidator = TableDataValidator.newInstance();
    tableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    tableDataValidator.setReporter(reporter);
    tableDataValidator.setValidationReporter(validationReporter);
    tableDataValidator.validate();

    final TableSchemaDefinitionValidator tableSchemaDefinitionValidator = TableSchemaDefinitionValidator.newInstance();
    tableSchemaDefinitionValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    tableSchemaDefinitionValidator.setReporter(reporter);
    tableSchemaDefinitionValidator.setValidationReporter(validationReporter);
    tableSchemaDefinitionValidator.validate();

    validationReporter.close();
  }

  /**
   * Normalize the exception into a ModuleException that is easier to understand
   * and handle.
   *
   * @param exception      The Exception that would otherwise be thrown
   * @param contextMessage
   * @return A normalized exception using ModuleException or one of its subclasses
   */
  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }

  private List<String> parseAllowUDTs(Path path) {
    if (path.toFile().exists() && path.toFile().isFile()) {
      List<String> lines = Collections.emptyList();
      try {
        lines = Files.readAllLines(path, StandardCharsets.UTF_8);
      } catch (IOException e) {
        e.printStackTrace();
      }

      return lines;
    }

    return Collections.emptyList();
  }
}
