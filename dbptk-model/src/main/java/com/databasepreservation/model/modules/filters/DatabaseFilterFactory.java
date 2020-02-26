/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.filters;

import java.util.Map;

import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

/**
 * Defines a factory used to create Filter Modules. This factory should also be
 * able to inform the parameters needed to create a new filter module.
 *
 * @author Tomás Ferreira <tferreira@keep.pt>
 */
public interface DatabaseFilterFactory {
  String getFilterName();

  ExecutionOrder getExecutionOrder();

  boolean isEnabled();

  Parameters getParameters();

  Map<String, Parameter> getAllParameters();

  DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;
}
