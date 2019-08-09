package com.databasepreservation.modules.siard.validate;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

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
import com.databasepreservation.modules.siard.validate.TableData.AdditionalChecksValidator;
import com.databasepreservation.modules.siard.validate.TableData.DateAndTimestampDataValidator;
import com.databasepreservation.modules.siard.validate.TableData.RequirementsForTableDataValidator;
import com.databasepreservation.modules.siard.validate.TableData.TableDataValidator;
import com.databasepreservation.modules.siard.validate.TableData.TableSchemaDefinitionValidator;
import com.databasepreservation.modules.siard.validate.common.path.ValidatorPathStrategy;
import com.databasepreservation.modules.siard.validate.common.path.ValidatorPathStrategyImpl;
import com.databasepreservation.modules.siard.validate.metadata.MetadataDatabaseInfoValidator;
import com.databasepreservation.modules.siard.validate.metadata.MetadataSchemaValidator;
import com.databasepreservation.modules.siard.validate.metadata.MetadataTypeValidator;
import com.databasepreservation.modules.siard.validate.metadata.MetadataXMLAgainstXSDValidator;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);
  private Reporter reporter;

  private final Path SIARDPackageNormalizedPath;
  private ValidationReporter validationReporter;
  private final List<String> allowedUDTs;
  private final ValidatorPathStrategy validatorPathStrategy;

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
    validatorPathStrategy = new ValidatorPathStrategyImpl();
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
    validatorPathStrategy = new ValidatorPathStrategyImpl();
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
    zipConstructionValidation.setValidatorPathStrategy(validatorPathStrategy);
    zipConstructionValidation.setup();
    zipConstructionValidation.validate();

    final SIARDStructureValidator siardStructureValidator = SIARDStructureValidator.newInstance();
    siardStructureValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    siardStructureValidator.setReporter(reporter);
    siardStructureValidator.setValidationReporter(validationReporter);
    siardStructureValidator.setValidatorPathStrategy(validatorPathStrategy);
    siardStructureValidator.setup();
    siardStructureValidator.validate();

    final MetadataAndTableDataValidator metadataAndTableDataValidator = MetadataAndTableDataValidator.newInstance();
    metadataAndTableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataAndTableDataValidator.setReporter(reporter);
    metadataAndTableDataValidator.setValidationReporter(validationReporter);
    metadataAndTableDataValidator.setAllowUDTs(allowedUDTs);
    metadataAndTableDataValidator.validate();

    final RequirementsForTableDataValidator requirementsForTableDataValidator = RequirementsForTableDataValidator.newInstance();
    requirementsForTableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    requirementsForTableDataValidator.setReporter(reporter);
    requirementsForTableDataValidator.setValidationReporter(validationReporter);
    requirementsForTableDataValidator.setValidatorPathStrategy(validatorPathStrategy);
    requirementsForTableDataValidator.validate();

    final TableSchemaDefinitionValidator tableSchemaDefinitionValidator = TableSchemaDefinitionValidator.newInstance();
    tableSchemaDefinitionValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    tableSchemaDefinitionValidator.setReporter(reporter);
    tableSchemaDefinitionValidator.setValidationReporter(validationReporter);
    tableSchemaDefinitionValidator.setValidatorPathStrategy(validatorPathStrategy);
    tableSchemaDefinitionValidator.validate();

    final DateAndTimestampDataValidator dateAndTimestampDataValidator = DateAndTimestampDataValidator.newInstance();
    dateAndTimestampDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    dateAndTimestampDataValidator.setReporter(reporter);
    dateAndTimestampDataValidator.setValidationReporter(validationReporter);
    dateAndTimestampDataValidator.setValidatorPathStrategy(validatorPathStrategy);
    dateAndTimestampDataValidator.validate();

    final TableDataValidator tableDataValidator = TableDataValidator.newInstance();
    tableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    tableDataValidator.setReporter(reporter);
    tableDataValidator.setValidationReporter(validationReporter);
    tableDataValidator.setValidatorPathStrategy(validatorPathStrategy);
    tableDataValidator.validate();

    final AdditionalChecksValidator additionalChecksValidator = AdditionalChecksValidator.newInstance();
    additionalChecksValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    additionalChecksValidator.setReporter(reporter);
    additionalChecksValidator.setValidationReporter(validationReporter);
    additionalChecksValidator.setValidatorPathStrategy(validatorPathStrategy);
    additionalChecksValidator.validate();

    final MetadataXMLAgainstXSDValidator metadataXMLAgainstXSDValidator = MetadataXMLAgainstXSDValidator.newInstance();

    metadataXMLAgainstXSDValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataXMLAgainstXSDValidator.setReporter(reporter);
    metadataXMLAgainstXSDValidator.setValidationReporter(validationReporter);
    metadataXMLAgainstXSDValidator.validate();

    final MetadataDatabaseInfoValidator metadataDatabaseInfoValidator = MetadataDatabaseInfoValidator.newInstance();

    metadataDatabaseInfoValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataDatabaseInfoValidator.setReporter(reporter);
    metadataDatabaseInfoValidator.setValidationReporter(validationReporter);
    metadataDatabaseInfoValidator.validate();

    final MetadataSchemaValidator metadataSchemaValidator = MetadataSchemaValidator.newInstance();
    metadataSchemaValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataSchemaValidator.setReporter(reporter);
    metadataSchemaValidator.setValidationReporter(validationReporter);
    metadataSchemaValidator.validate();

    final MetadataTypeValidator metadataTypeValidator = MetadataTypeValidator.newInstance();
    metadataTypeValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
    metadataTypeValidator.setReporter(reporter);
    metadataTypeValidator.setValidationReporter(validationReporter);
    metadataTypeValidator.validate();

    try {
      validationReporter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
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
