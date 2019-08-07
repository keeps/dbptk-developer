package com.databasepreservation.modules.siard.validate;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);
  private Reporter reporter;

  private final Path SIARDPackageNormalizedPath;
  private ValidationReporter validationReporter;

  /**
   * Constructor used to initialize required objects to get an validate module
   * for SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path SIARDPackagePath, Path validationReporterPath) {
    SIARDPackageNormalizedPath = SIARDPackagePath.toAbsolutePath().normalize();
    validationReporter = new ValidationReporter(validationReporterPath.toAbsolutePath().normalize(), SIARDPackageNormalizedPath);
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
    zipConstructionValidation.setOnceReporter(reporter);
    zipConstructionValidation.setValidationReporter(validationReporter);
    final boolean validate = zipConstructionValidation.validate();

    validationReporter.close();

    System.out.println(validate);
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
}
