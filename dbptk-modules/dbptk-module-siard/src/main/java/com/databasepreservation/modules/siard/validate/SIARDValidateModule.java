package com.databasepreservation.modules.siard.validate;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.ValidationObserver;
import com.databasepreservation.common.ValidatorPathStrategy;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.components.ValidatorComponent;
import com.databasepreservation.model.components.ValidatorComponentFactory;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.modules.siard.validate.common.path.ValidatorPathStrategyImpl;
import com.databasepreservation.utils.ReflectionUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);
  private Reporter reporter;
  private ValidationObserver observer;
  private final Path SIARDPackageNormalizedPath;
  private ValidationReporter validationReporter;
  private final List<String> allowedUDTs;
  private final ValidatorPathStrategy validatorPathStrategy;

  /**
   * Constructor used to initialize required objects to get an validate module for
   * SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath
   *          Path to the main SIARD file (file with extension .siard)
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
   * @param reporter
   *          The reporter that should be used by this ValidateModule
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public void setObserver(ValidationObserver observer) {
    this.observer = observer;
  }

  /**
   * @throws ModuleException
   *           Generic module exception
   */
  @Override
  public void validate() throws ModuleException {

    List<ValidatorComponent> components = getValidationComponents();

    for (ValidatorComponent component : components) {
      component.setReporter(reporter);
      component.setSIARDPath(SIARDPackageNormalizedPath);
      component.setValidationReporter(validationReporter);
      component.setValidatorPathStrategy(validatorPathStrategy);
      component.setAllowedUTD(allowedUDTs);
      component.setup();
      final boolean validate = component.validate();
    }

    /*
     * final ZipConstructionValidator zipConstructionValidation =
     * ZipConstructionValidator.newInstance();
     * 
     * zipConstructionValidation.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * zipConstructionValidation.setReporter(reporter);
     * zipConstructionValidation.setValidationReporter(validationReporter);
     * zipConstructionValidation.setValidatorPathStrategy(validatorPathStrategy);
     * zipConstructionValidation.setup(); zipConstructionValidation.validate();
     * 
     * final SIARDStructureValidator siardStructureValidator =
     * SIARDStructureValidator.newInstance();
     * siardStructureValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * siardStructureValidator.setReporter(reporter);
     * siardStructureValidator.setValidationReporter(validationReporter);
     * siardStructureValidator.setValidatorPathStrategy(validatorPathStrategy);
     * siardStructureValidator.setup(); siardStructureValidator.validate();
     * 
     * final MetadataAndTableDataValidator metadataAndTableDataValidator =
     * MetadataAndTableDataValidator.newInstance();
     * metadataAndTableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath)
     * ; metadataAndTableDataValidator.setReporter(reporter);
     * metadataAndTableDataValidator.setValidationReporter(validationReporter);
     * metadataAndTableDataValidator.setAllowUDTs(allowedUDTs);
     * metadataAndTableDataValidator.validate();
     * 
     * final RequirementsForTableDataValidator requirementsForTableDataValidator =
     * RequirementsForTableDataValidator.newInstance();
     * requirementsForTableDataValidator.setSIARDPackagePath(
     * SIARDPackageNormalizedPath);
     * requirementsForTableDataValidator.setReporter(reporter);
     * requirementsForTableDataValidator.setValidationReporter(validationReporter);
     * requirementsForTableDataValidator.setValidatorPathStrategy(
     * validatorPathStrategy); requirementsForTableDataValidator.validate();
     * 
     * final TableSchemaDefinitionValidator tableSchemaDefinitionValidator =
     * TableSchemaDefinitionValidator.newInstance();
     * tableSchemaDefinitionValidator.setSIARDPackagePath(SIARDPackageNormalizedPath
     * ); tableSchemaDefinitionValidator.setReporter(reporter);
     * tableSchemaDefinitionValidator.setValidationReporter(validationReporter);
     * tableSchemaDefinitionValidator.setValidatorPathStrategy(validatorPathStrategy
     * ); tableSchemaDefinitionValidator.validate();
     * 
     * final DateAndTimestampDataValidator dateAndTimestampDataValidator =
     * DateAndTimestampDataValidator.newInstance();
     * dateAndTimestampDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath)
     * ; dateAndTimestampDataValidator.setReporter(reporter);
     * dateAndTimestampDataValidator.setValidationReporter(validationReporter);
     * dateAndTimestampDataValidator.setValidatorPathStrategy(validatorPathStrategy)
     * ; dateAndTimestampDataValidator.validate();
     * 
     * final TableDataValidator tableDataValidator =
     * TableDataValidator.newInstance();
     * tableDataValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * tableDataValidator.setReporter(reporter);
     * tableDataValidator.setValidationReporter(validationReporter);
     * tableDataValidator.setValidatorPathStrategy(validatorPathStrategy);
     * tableDataValidator.validate();
     * 
     * final AdditionalChecksValidator additionalChecksValidator =
     * AdditionalChecksValidator.newInstance();
     * additionalChecksValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * additionalChecksValidator.setReporter(reporter);
     * additionalChecksValidator.setValidationReporter(validationReporter);
     * additionalChecksValidator.setValidatorPathStrategy(validatorPathStrategy);
     * additionalChecksValidator.validate();
     * 
     * final MetadataXMLAgainstXSDValidator metadataXMLAgainstXSDValidator =
     * MetadataXMLAgainstXSDValidator.newInstance();
     * 
     * metadataXMLAgainstXSDValidator.setSIARDPackagePath(SIARDPackageNormalizedPath
     * ); metadataXMLAgainstXSDValidator.setReporter(reporter);
     * metadataXMLAgainstXSDValidator.setValidationReporter(validationReporter);
     * metadataXMLAgainstXSDValidator.validate();
     * 
     * final MetadataDatabaseInfoValidator metadataDatabaseInfoValidator =
     * MetadataDatabaseInfoValidator.newInstance();
     * 
     * metadataDatabaseInfoValidator.setSIARDPackagePath(SIARDPackageNormalizedPath)
     * ; metadataDatabaseInfoValidator.setReporter(reporter);
     * metadataDatabaseInfoValidator.setValidationReporter(validationReporter);
     * metadataDatabaseInfoValidator.validate();
     * 
     * final MetadataSchemaValidator metadataSchemaValidator =
     * MetadataSchemaValidator.newInstance();
     * metadataSchemaValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * metadataSchemaValidator.setReporter(reporter);
     * metadataSchemaValidator.setValidationReporter(validationReporter);
     * metadataSchemaValidator.validate();
     * 
     * final MetadataTypeValidator metadataTypeValidator =
     * MetadataTypeValidator.newInstance();
     * metadataTypeValidator.setSIARDPackagePath(SIARDPackageNormalizedPath);
     * metadataTypeValidator.setReporter(reporter);
     * metadataTypeValidator.setValidationReporter(validationReporter);
     * metadataTypeValidator.validate();
     * 
     * startValidation(MetadataXMLAgainstXSDValidator.newInstance());
     * startValidation(MetadataDatabaseInfoValidator.newInstance());
     * startValidation(MetadataSchemaValidator.newInstance());
     * startValidation(MetadataTypeValidator.newInstance());
     * startValidation(MetadataAttributeValidator.newInstance());
     * startValidation(MetadataTableValidator.newInstance());
     * startValidation(MetadataColumnsValidator.newInstance());
     * startValidation(MetadataFieldValidator.newInstance());
     * startValidation(MetadataPrimaryKeyValidator.newInstance());
     * startValidation(MetadataForeignKeyValidator.newInstance());
     * startValidation(MetadataReferenceValidator.newInstance());
     * startValidation(MetadataCandidateKeyValidator.newInstance());
     * startValidation(MetadataCheckConstraintValidator.newInstance());
     * startValidation(MetadataTriggerValidator.newInstance());
     * startValidation(MetadataViewValidator.newInstance());
     * startValidation(MetadataRoutineValidator.newInstance());
     * startValidation(MetadataParameterValidator.newInstance());
     * startValidation(MetadataUserValidator.newInstance());
     * startValidation(MetadataRoleValidator.newInstance());
     * startValidation(MetadataPrivilegeValidator.newInstance());
     */

    try {
      validationReporter.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  /*
   * private void startValidation(ValidatorComponentImpl module) throws
   * ModuleException { module.setSIARDPackagePath(SIARDPackageNormalizedPath);
   * module.setReporter(reporter);
   * module.setValidationReporter(validationReporter); module.validate(); }
   */

  /**
   * Normalize the exception into a ModuleException that is easier to understand
   * and handle.
   *
   * @param exception
   *          The Exception that would otherwise be thrown
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

  private List<ValidatorComponent> getValidationComponents() throws ModuleException {
    List<ValidatorComponent> components = new ArrayList<>();

    final Collection<ValidatorComponentFactory> validatorComponentFactories = ReflectionUtils
      .collectValidatorComponentFactories(false);

    List<ValidatorComponentFactory> order = new ArrayList<>();
    ValidatorComponentFactory next = null;
    for (ValidatorComponentFactory factory : validatorComponentFactories) {
      if (factory.isFirst()) {
        order.add(factory);
        next = getFactory(validatorComponentFactories, factory.next());
      }
    }

    while (order.size() < validatorComponentFactories.size()) {
      order.add(next);
      if (next != null) {
        next = getFactory(validatorComponentFactories, next.next());
      }
    }

    for (ValidatorComponentFactory factory : order) {
      components.add(factory.buildComponent(reporter));
    }

    return components;
  }

  private ValidatorComponentFactory getFactory(Collection<ValidatorComponentFactory> factories, String componentName) {
    for (ValidatorComponentFactory factory : factories) {
      if (factory.getComponentName().equals(componentName)) {
        return factory;
      }
    }
    return null;
  }
}
