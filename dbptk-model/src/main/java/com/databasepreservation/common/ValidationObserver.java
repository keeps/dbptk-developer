/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common;

import com.databasepreservation.model.reporters.ValidationReporter.Status;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidationObserver {
  void notifyStartValidationModule(String moduleName, String ID);

  void notifyValidationStep(String moduleName, String step, Status status);

  void notifyMessage(String moduleName, String message, Status status);

  void notifyFinishValidationModule(String moduleName, Status status);

  void notifyComponent(String ID, Status status);

  void notifyElementValidating(String path);
}
