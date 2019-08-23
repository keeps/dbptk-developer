package com.databasepreservation.modules.siard.validate.component.factories;

import com.databasepreservation.Constants;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.components.ValidatorComponent;
import com.databasepreservation.model.components.ValidatorComponentFactory;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.validate.component.formatStructure.ZipConstructionValidator;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ZipConstructionComponentFactory implements ValidatorComponentFactory {
  private final String MODULE_NAME = Constants.COMPONENT_ZIP_CONSTRUCTION;

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
    return true;
  }

  @Override
  public boolean isFirst() {
    return true;
  }

  @Override
  public String next() {
    return Constants.COMPONENT_SIARD_STRUCTURE;
  }

  @Override
  public ValidatorComponent buildComponent(Reporter reporter) throws ModuleException {
    return new ZipConstructionValidator(MODULE_NAME);
  }
}
