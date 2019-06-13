/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;


import org.apache.commons.cli.ParseException;

/**
 * Exception thrown when a number of arguments for an option exceed the max limit.
 * This exception extends the behaviour of {@link ParseException}.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class TooMuchArgumentsException extends ParseException {

  /**
   * Construct a new <code>ParseException</code>
   * with the specified detail message.
   *
   * @param message the detail message
   */
  public TooMuchArgumentsException(String message) {
    super(message);
  }
}
