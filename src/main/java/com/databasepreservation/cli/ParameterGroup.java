package com.databasepreservation.cli;

import org.apache.commons.cli.OptionGroup;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

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

  public OptionGroup toOptionGroup(String prefix) {
    OptionGroup optionGroup = null;
    if ((optionGroup = optionGroups.get(prefix)) == null) {
      optionGroup = new OptionGroup();
      optionGroup.setRequired(required);
      for (Parameter parameter : parameters) {
        optionGroup.addOption(parameter.toOption(prefix));
      }
    }
    return optionGroup;
  }
}
