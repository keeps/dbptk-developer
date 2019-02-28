/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * Exception about problems connecting to the server (except if they are
 * permission problems, in which case the PermissionDeniedException is used).
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ConnectionException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Problem in database connection. ";
  }
}
