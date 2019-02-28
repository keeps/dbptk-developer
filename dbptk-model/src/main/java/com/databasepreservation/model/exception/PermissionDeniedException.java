/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * Exceptions related to not having the required permissions to execute the
 * action. Usually granting the appropriate permissions (via GRANT, chmod, etc)
 * should be enough to fix these kind of errors.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PermissionDeniedException extends ModuleException {
  @Override
  protected String messagePrefix() {
    return "Permission denied. ";
  }
}
