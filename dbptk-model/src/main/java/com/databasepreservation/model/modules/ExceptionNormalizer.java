package com.databasepreservation.model.modules;

import com.databasepreservation.model.exception.ModuleException;

/**
 * Handles exceptions in a way that only the specific database import/export
 * module can. Implementations of this should try to determine the DB-specific
 * cause for the exception and create an appropriate ModuleException (or
 * subclass of it).
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ExceptionNormalizer {

  /**
   * Normalize the exception into a ModuleException that is easier to understand
   * and handle.
   * 
   * @param exception
   *          The Exception that would otherwise be thrown
   * @param contextMessage
   * @return A normalized exception using ModuleException or one of its subclasses
   */
  ModuleException normalizeException(Exception exception, String contextMessage);
}
