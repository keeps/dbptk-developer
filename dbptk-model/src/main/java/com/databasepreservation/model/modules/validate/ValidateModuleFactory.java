/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.validate;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

import java.util.Map;

/**
 *  Defines a factory used to create Validate Modules. This factory
 *  should also be able to inform the parameters needed to create a new validate module.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidateModuleFactory {

  /**
   * Returns the module name.
   *
   * @return The module name.
   */
  String getModuleName();

  /**
   * Returns the state of this factory.
   *
   * @return true if enabled otherwise false.
   */
  boolean isEnabled();

  /**
   * Returns <code>Parameters</code>
   *
   * @return The {@link Parameters} for the specific factory
   */
  Parameters getParameters();

  Parameters getSingleParameters();

  /**
   * Returns a combination of all <code>Parameter</code> and its values.
   *
   * @return a map with the pair {@link Parameter}, {@link String}.
   */
  Map<String, Parameter> getAllParameters();

  /**
   * Builds the specific <code>ValidateModule</code> according the input parameters
   *
   * @param parameters
   *          A Map with the input parameters
   * @param reporter
   *          The reporter that should be used by the <code>EditModule</code>
   * @return Returns a specific {@link ValidateModule}
   * @throws ModuleException
   *          Generic module exception
   */
  ValidateModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;
}
