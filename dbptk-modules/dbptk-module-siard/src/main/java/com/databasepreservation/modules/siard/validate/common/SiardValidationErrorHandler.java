/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common;

import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.util.ArrayList;
import java.util.List;

/**
 * Custom ErrorHandler for SIARD XML validation that captures detailed error
 * information including line numbers, column numbers, and error messages.
 * 
 * @author Generated for enhanced SIARD validation diagnostics
 */
public class SiardValidationErrorHandler implements ErrorHandler {
  
  private final List<ValidationError> errors = new ArrayList<>();
  private final List<ValidationError> warnings = new ArrayList<>();
  private final List<ValidationError> fatalErrors = new ArrayList<>();

  @Override
  public void warning(SAXParseException exception) throws SAXException {
    warnings.add(new ValidationError(
      ErrorType.WARNING,
      exception.getLineNumber(),
      exception.getColumnNumber(),
      exception.getMessage(),
      exception
    ));
  }

  @Override
  public void error(SAXParseException exception) throws SAXException {
    errors.add(new ValidationError(
      ErrorType.ERROR,
      exception.getLineNumber(),
      exception.getColumnNumber(),
      exception.getMessage(),
      exception
    ));
  }

  @Override
  public void fatalError(SAXParseException exception) throws SAXException {
    fatalErrors.add(new ValidationError(
      ErrorType.FATAL_ERROR,
      exception.getLineNumber(),
      exception.getColumnNumber(),
      exception.getMessage(),
      exception
    ));
    // Fatal errors should stop processing
    throw exception;
  }

  public boolean hasErrors() {
    return !errors.isEmpty() || !fatalErrors.isEmpty();
  }

  public boolean hasWarnings() {
    return !warnings.isEmpty();
  }

  public List<ValidationError> getErrors() {
    return errors;
  }

  public List<ValidationError> getWarnings() {
    return warnings;
  }

  public List<ValidationError> getFatalErrors() {
    return fatalErrors;
  }

  public List<ValidationError> getAllErrors() {
    List<ValidationError> allErrors = new ArrayList<>();
    allErrors.addAll(fatalErrors);
    allErrors.addAll(errors);
    return allErrors;
  }

  /**
   * Represents a validation error with detailed context information
   */
  public static class ValidationError {
    private final ErrorType type;
    private final int lineNumber;
    private final int columnNumber;
    private final String message;
    private final SAXParseException exception;

    public ValidationError(ErrorType type, int lineNumber, int columnNumber, String message, SAXParseException exception) {
      this.type = type;
      this.lineNumber = lineNumber;
      this.columnNumber = columnNumber;
      this.message = message;
      this.exception = exception;
    }

    public ErrorType getType() {
      return type;
    }

    public int getLineNumber() {
      return lineNumber;
    }

    public int getColumnNumber() {
      return columnNumber;
    }

    public String getMessage() {
      return message;
    }

    public SAXParseException getException() {
      return exception;
    }
  }

  /**
   * Enum representing the type of validation error
   */
  public enum ErrorType {
    WARNING,
    ERROR,
    FATAL_ERROR
  }
}
