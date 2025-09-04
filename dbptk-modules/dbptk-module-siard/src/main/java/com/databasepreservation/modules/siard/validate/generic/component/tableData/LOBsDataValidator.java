/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.generic.component.tableData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.modules.siard.validate.generic.component.ValidatorComponentImpl;

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
    getValidationReporter().moduleValidatorHeader(P_62, MODULE_NAME);

    observer.notifyFinishValidationModule(MODULE_NAME, ValidationReporterStatus.PASSED);
    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporterStatus.PASSED);

    return true;
  }
}
