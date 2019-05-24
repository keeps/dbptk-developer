/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.edits;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

import java.util.Map;

/**
 *  Defines a factory used to create Edit Modules. This factory
 *  should also be able to inform the parameters needed to create a new edit module.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface EditModuleFactory {

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
   * Returns the import <code>Parameters</code>
   *
   * @return The {@link Parameters} for the specific factory
   */
  Parameters getImportParameters();

  /**
   * Returns <code>Parameters</code>
   *
   * @return The {@link Parameters} for the specific factory
   */
  Parameters getParameters();

  /**
   * Returns a combination of <code>Parameter</code> and its values for the 'set' option
   *
   * @return a map with the pair {@link Parameter}, {@link String}.
   */
  Map<Parameter, String> getSetParameters();

  /**
   * Returns a combination of all <code>Parameter</code> and its values.
   *
   * @return a map with the pair {@link Parameter}, {@link String}.
   */
  Map<Parameter, String> getAllParameters();

  /**
   * Builds the specific <code>EditModule</code> according the input parameters
   *
   * @param parameters
   *          A Map with the input parameters
   * @param reporter
   *          The reporter that should be used by the <code>EditModule</code>
   * @return Returns a specific {@link EditModule}
   * @throws ModuleException
   *          Generic module exception
   */
  EditModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;
}
