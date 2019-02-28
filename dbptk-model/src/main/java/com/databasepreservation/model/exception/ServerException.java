/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * Exceptions that probably require the database administrator to fix them. eg:
 * configuration problems, quota exceeded
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ServerException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Problem in database server. ";
  }
}
