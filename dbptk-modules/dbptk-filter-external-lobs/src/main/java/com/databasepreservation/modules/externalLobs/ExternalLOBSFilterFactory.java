/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;

public class ExternalLOBSFilterFactory implements DatabaseFilterFactory {

  @Override
  public String getFilterName() {
    return "external-lobs";
  }

  @Override
  public String getExecutionOrder() {
    return "before";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Collections.emptyList(), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    return new HashMap<>();
  }

  @Override
  public DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter) {
    return new ExternalLOBSFilter();
  }
}
