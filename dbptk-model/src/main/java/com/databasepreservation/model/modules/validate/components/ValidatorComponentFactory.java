/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.validate.components;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;

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
