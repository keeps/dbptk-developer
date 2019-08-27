package com.databasepreservation.modules.siard.validate.component.tableData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter.Status;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class LOBsDataValidator extends ValidatorComponentImpl {
  private static final Logger LOGGER = LoggerFactory.getLogger(LOBsDataValidator.class);

  private final String MODULE_NAME;
  private static final String P_62 = "T_6.2";
  private static final String P_601 = "T_6.2-1";
  private static final String P_6211 = "T_6.2-1-1";
  private static final String P_6212 = "T_6.2-1-2";
  private static final String P_6213 = "T_6.2-1-3";

  public LOBsDataValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, P_62);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(P_62, MODULE_NAME);

    observer.notifyFinishValidationModule(MODULE_NAME, Status.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, Status.PASSED);
    closeZipFile();

    return true;
  }
}
