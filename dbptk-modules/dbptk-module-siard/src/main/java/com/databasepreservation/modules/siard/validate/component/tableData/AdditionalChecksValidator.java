/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.tableData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class AdditionalChecksValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalChecksValidator.class);

  private final String MODULE_NAME;
  private static final String A_10 = "A 1.0";


  public AdditionalChecksValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public void clean() {
    // Nothing to do
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, A_10);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }


    getValidationReporter().moduleValidatorHeader(MODULE_NAME);



    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);
    closeZipFile();

    return true;
  }
}
