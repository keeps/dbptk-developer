/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.content;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Class to build SAX Parsing errors
 *
 * @author bferreira
 */
class SAXErrorHandler implements ErrorHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SAXErrorHandler.class);
  private boolean hasError = false;

  /**
   * @return true if the error handler contains error information
   */
  public boolean hasError() {
    return hasError;
  }

  private String getParseExceptionInfo(SAXParseException e) {
    StringBuilder buf = new StringBuilder();

    if (e.getPublicId() != null) {
      buf.append("publicId: ").append(e.getPublicId()).append("; ");
    }

    if (e.getSystemId() != null) {
      buf.append("systemId: ").append(e.getSystemId()).append("; ");
    }

    if (e.getLineNumber() != -1) {
      buf.append("line: ").append(e.getLineNumber()).append("; ");
    }

    if (e.getColumnNumber() != -1) {
      buf.append("column: ").append(e.getColumnNumber()).append("; ");
    }

    if (e.getLocalizedMessage() != null) {
      buf.append(e.getLocalizedMessage());
    }

    return buf.toString();
  }

  /**
   * Logs SAX warnings with the custom LOGGER.
   * 
   * @param e
   *          the original exception
   */
  @Override
  public void warning(SAXParseException e) {
    LOGGER.warn(getParseExceptionInfo(e), e);
  }

  /**
   * Logs SAX errors with the custom LOGGER.
   * 
   * @param e
   *          the original exception
   */
  @Override
  public void error(SAXParseException e) {
    LOGGER.error(getParseExceptionInfo(e), e);
    hasError = true;
  }

  /**
   * Rethrows fatal SAX errors with a better error message
   * 
   * @param e
   *          The original exception
   * @throws SAXException
   *           An exception with a better error message. The cause attribute of
   *           this exception is the original exception
   */
  @Override
  public void fatalError(SAXParseException e) throws SAXException {
    hasError = true;
    throw new SAXException(String.format("Fatal Error: %s", getParseExceptionInfo(e)), e);
  }
}
