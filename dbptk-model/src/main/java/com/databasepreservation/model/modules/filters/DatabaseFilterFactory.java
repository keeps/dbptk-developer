package com.databasepreservation.model.modules.filters;

import java.util.Map;

import com.databasepreservation.model.Reporter;
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

  boolean isEnabled();

  Parameters getParameters();

  Map<String, Parameter> getAllParameters();

  DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;
}
