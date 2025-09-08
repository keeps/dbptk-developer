/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.siard_21.component.factories;

import com.databasepreservation.Constants;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.modules.validate.components.ValidatorComponent;
import com.databasepreservation.model.modules.validate.components.ValidatorComponentFactory;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.validate.siard_21.component.formatStructure.SIARDStructureValidator;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDStructureComponentFactory implements ValidatorComponentFactory {
  private final String MODULE_NAME = Constants.COMPONENT_SIARD_STRUCTURE;

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
    return false;
  }

  @Override
  public String next() {
    return Constants.COMPONENT_METADATA_AND_TABLE_DATA;
  }

  @Override
  public ValidatorComponent buildComponent(Reporter reporter) throws ModuleException {
    return new SIARDStructureValidator(MODULE_NAME);
  }
}
