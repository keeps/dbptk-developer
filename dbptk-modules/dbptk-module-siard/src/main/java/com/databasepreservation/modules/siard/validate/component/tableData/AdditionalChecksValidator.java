package com.databasepreservation.modules.siard.validate.component.tableData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
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
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, A_10);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }


    getValidationReporter().moduleValidatorHeader(MODULE_NAME);



    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }
}
