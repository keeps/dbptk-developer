/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.postgreSql;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.util.PSQLException;

import com.databasepreservation.model.exception.ConnectionException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.PermissionDeniedException;
import com.databasepreservation.model.exception.ServerException;
import com.databasepreservation.model.modules.ExceptionNormalizer;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSQLExceptionNormalizer implements ExceptionNormalizer {
  private static final PostgreSQLExceptionNormalizer instance = new PostgreSQLExceptionNormalizer();

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
    if (exception instanceof PSQLException) {
      PSQLException e = (PSQLException) exception;

      if (StringUtils.isNotBlank(e.getSQLState()) && e.getSQLState().length() == 5) {
        String errorCode = e.getSQLState();
        String codeClass = errorCode.substring(0, 2);

        String message = e.getMessage();
        if (message != null) {
          message = message.replaceAll("(.*?: )?(.*?)(\\n[\\s\\S]*)?", "$2");
        } else {
          message = contextMessage;
        }

        // see: https://www.postgresql.org/docs/10/errcodes-appendix.html

        // handle code classes
        switch (codeClass) {
          case "08": // Class 08 - Connection Exception
            return new ConnectionException().withCause(e).withMessage(message);

          case "28": // Class 28 - Invalid Authorization Specification
            return new PermissionDeniedException().withCause(e).withMessage(message);

          case "53": // Class 53 - Insufficient Resources
          case "54": // Class 54 - Program Limit Exceeded
          case "57": // Class 57 - Operator Intervention
          case "58": // Class 58 — System Error (errors external to PostgreSQL itself)
          case "F0": // Class F0 - Configuration File Error
          case "XX": // Class XX - Internal Error
            return new ServerException().withCause(e).withMessage(message);
        }

        // handle specific error codes when the error class has more than one kind of
        // error
        switch (errorCode) {
          case "38004": // 38 - External Routine Exception; 38004 - reading_sql_data_not_permitted
          case "42501": // 42 - Syntax Error or Access Rule Violation; 42501 - insufficient_privilege
            return new PermissionDeniedException().withCause(e).withMessage(message);
        }
      }
    }

    return null;
  }
}
