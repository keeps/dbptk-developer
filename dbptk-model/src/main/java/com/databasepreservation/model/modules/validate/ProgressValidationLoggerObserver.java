/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.validate;

import com.databasepreservation.common.ValidationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.databasepreservation.model.reporters.ValidationReporter.Status;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ProgressValidationLoggerObserver implements ValidationObserver {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressValidationLoggerObserver.class);

  @Override
  public void notifyStartValidationModule(String moduleName, String ID) {
    LOGGER.info("Start validation of: {} - {}", ID, moduleName);
  }

  @Override
  public void notifyValidationStep(String moduleName, String step, Status status) {
    LOGGER.info("{}: [{}]", step, status);
  }

  @Override
  public void notifyMessage(String moduleName, String message, Status status) {
    LOGGER.info("{}: [{}] - {}", moduleName, status, message);
  }

  @Override
  public void notifyFinishValidationModule(String moduleName, Status status) {
    LOGGER.info("{} [{}]", moduleName, status);
  }

  @Override
  public void notifyComponent(String ID, Status status) {
    LOGGER.info("{}: [{}] ", ID, status);
  }

  @Override
  public void notifyElementValidating(String path) {
    LOGGER.info("{}", path);
  }
}
