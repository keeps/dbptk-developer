package com.databasepreservation.modules;

import java.sql.SQLException;

import com.databasepreservation.model.exception.DriverNotFoundException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.ExceptionNormalizer;

/**
 * Tries to handle exceptions that are not specific to some database system and
 * exceptions that other ExceptionNormalizers were not able to handle.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DefaultExceptionNormalizer implements ExceptionNormalizer {
  private static final DefaultExceptionNormalizer instance = new DefaultExceptionNormalizer();

  /**
   * Although this class is not necessarily a singleton, it can be used like a
   * singleton to avoid creating multiple (similar) instances.
   * 
   * @return an ExceptionNormalizer
   */
  public static ExceptionNormalizer getInstance() {
    return instance;
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    if (exception instanceof ModuleException) {
      return (ModuleException) exception;
    }

    if (exception instanceof ClassNotFoundException) {
      return new DriverNotFoundException().withCause(exception);
    }

    if (exception instanceof SQLException) {

    }

    return new ModuleException().withCause(exception);
  }
}
