package com.databasepreservation.modules.siard.validate;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private Reporter reporter;
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);

  /**
   * Constructor used to initialize required objects to get an validate module
   * for SIARD 2 (all minor versions)
   *
   * @param siardPackagePath Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path siardPackagePath) {
    Path siardPackageNormalizedPath = siardPackagePath.toAbsolutePath().normalize();

    // TODO: initializations

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
    // TODO: validation
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
