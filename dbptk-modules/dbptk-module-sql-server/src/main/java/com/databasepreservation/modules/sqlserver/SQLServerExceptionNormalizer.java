/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.sqlserver;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.PermissionDeniedException;
import com.databasepreservation.model.modules.ExceptionNormalizer;
import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLServerExceptionNormalizer implements ExceptionNormalizer {
  private static final SQLServerExceptionNormalizer instance = new SQLServerExceptionNormalizer();

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
    if (exception instanceof SQLServerException) {
      SQLServerException e = (SQLServerException) exception;

      if (e.getSQLState() == null) {
        return new ModuleException().withMessage(e.getMessage()).withCause(e);
      }

      if (e.getSQLState().equals("S0001")) {
        return new PermissionDeniedException().withCause(e);
      }
    }

    return null;
  }
}
