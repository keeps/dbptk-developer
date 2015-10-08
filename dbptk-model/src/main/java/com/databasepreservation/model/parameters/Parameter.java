package com.databasepreservation.model.parameters;

import java.util.HashMap;

import org.apache.commons.cli.Option;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Parameter {
  private String shortName = null;
  private String longName = null;
  private String description = null;
  private boolean hasArgument = false;
  private boolean optionalArgument = false;
  private boolean required = false;
  private String valueIfSet = null; // for parameters without argument
  private String valueIfNotSet = null; // for optional parameters that were not
                                       // set

  private HashMap<String, Option> options = new HashMap<String, Option>();

  public Parameter() {
  }

  public String description() {
    return description;
  }

  public Parameter description(String description) {
    this.description = description;
    return this;
  }

  public boolean hasArgument() {
    return hasArgument;
  }

  public Parameter hasArgument(boolean hasArgument) {
    this.hasArgument = hasArgument;
    return this;
  }

  public boolean isOptionalArgument() {
    return optionalArgument;
  }

  public Parameter setOptionalArgument(boolean optionalArgument) {
    this.optionalArgument = optionalArgument;
    return this;
  }

  public String longName() {
    return longName;
  }

  public Parameter longName(String longName) {
    this.longName = longName;
    return this;
  }

  public boolean required() {
    return required;
  }

  public Parameter required(boolean required) {
    this.required = required;
    return this;
  }

  public String shortName() {
    return shortName;
  }

  public Parameter shortName(String shortName) {
    this.shortName = shortName;
    return this;
  }

  public String valueIfNotSet() {
    return valueIfNotSet;
  }

  public Parameter valueIfNotSet(String valueIfNotSet) {
    this.valueIfNotSet = valueIfNotSet;
    return this;
  }

  public String valueIfSet() {
    return valueIfSet;
  }

  public Parameter valueIfSet(String valueIfSet) {
    this.valueIfSet = valueIfSet;
    return this;
  }

  public Option toOption(String shortNamePrefix, String longNamePrefix) {
    Option option = null;
    String optionID = shortNamePrefix + " " + longNamePrefix;

    if (longName == null) {
      throw new RuntimeException("Parameter has no long name. All Parameter instances must have a long name.");
    }

    if ((option = options.get(optionID)) == null) {
      Option.Builder optionBuilder = Option.builder();

      if (shortName != null) {
        optionBuilder = Option.builder(shortNamePrefix + shortName);
      }

      option = optionBuilder.longOpt(longNamePrefix + "-" + longName).desc(description).hasArg(hasArgument)
        .required(required).optionalArg(optionalArgument).build();

      options.put(optionID, option);
    }

    return option;
  }
}
