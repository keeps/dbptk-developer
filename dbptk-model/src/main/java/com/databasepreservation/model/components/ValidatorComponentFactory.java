package com.databasepreservation.model.components;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidatorComponentFactory {
  /**
   * Gets the component name.
   *
   * @return The component name.
   */
  String getComponentName();

  /**
   * Returns the state of this factory.
   *
   * @return true if enabled otherwise false.
   */
  boolean isEnabled();

  boolean isFirst();

  String next();

  ValidatorComponent buildComponent(Reporter reporter) throws ModuleException;
}
