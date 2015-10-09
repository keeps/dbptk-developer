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
  private HashMap<String, OptionGroup> optionGroups = new HashMap<String, OptionGroup>();

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
