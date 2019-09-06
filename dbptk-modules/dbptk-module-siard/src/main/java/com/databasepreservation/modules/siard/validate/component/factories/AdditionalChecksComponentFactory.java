package com.databasepreservation.modules.siard.validate.component.factories;

import com.databasepreservation.Constants;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.modules.validate.components.ValidatorComponent;
import com.databasepreservation.model.modules.validate.components.ValidatorComponentFactory;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.validate.component.tableData.AdditionalChecksValidator;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class AdditionalChecksComponentFactory implements ValidatorComponentFactory {
  private final String MODULE_NAME = Constants.COMPONENT_ADDITIONAL_CHECKS;

  /**
   * Gets the component name.
   *
   * @return The component name.
   */
  @Override
  public String getComponentName() {
    return MODULE_NAME;
  }

  /**
   * Returns the state of this factory.
   *
   * @return true if enabled otherwise false.
   */
  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public boolean isFirst() {
    return false;
  }

  @Override
  public String next() {
    return Constants.COMPONENT_END_TAG;
  }

  @Override
  public ValidatorComponent buildComponent(Reporter reporter) throws ModuleException {
    return new AdditionalChecksValidator(MODULE_NAME);
  }
}
