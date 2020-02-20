/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.parameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.OptionGroup;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ParameterGroup {
  private final boolean required;
  private final List<Parameter> parameters;
  private HashMap<String, OptionGroup> optionGroups = new HashMap<>();

  public ParameterGroup(boolean required, Parameter... parameters) {
    this.required = required;
    this.parameters = Arrays.asList(parameters);
  }

  public List<Parameter> getParameters() {
    return parameters;
  }

  public OptionGroup toOptionGroup(String shortNamePrefix, String longNamePrefix) {
    OptionGroup optionGroup = null;
    String optionGroupID = shortNamePrefix + " " + longNamePrefix;

    if ((optionGroup = optionGroups.get(optionGroupID)) == null) {
      optionGroup = new OptionGroup();
      optionGroup.setRequired(required);
      for (Parameter parameter : parameters) {
        optionGroup.addOption(parameter.toOption(shortNamePrefix, longNamePrefix));
      }
    }
    return optionGroup;
  }
}
