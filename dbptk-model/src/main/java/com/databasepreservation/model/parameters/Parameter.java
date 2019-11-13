/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.parameters;

import java.util.HashMap;

import org.apache.commons.cli.Option;

/**
 * Command line parameter specification.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Parameter {

  /* GUI Helper */
  public enum INPUT_TYPE {
    DRIVER, FILE_SAVE, FILE_OPEN, FOLDER, TEXT, PASSWORD, CHECKBOX, NUMBER, DEFAULT, NONE
  }

  /* GUI Helper for SIARD Export Module */
  public enum CATEGORY_TYPE {
    SIARD_EXPORT_OPTIONS, METADATA_EXPORT_OPTIONS, EXTERNAL_LOBS, NONE
  }

  public enum FILE_FILTER_TYPE {
    XML_EXTENSION, SIARD_EXTENSION
  }

  private String shortName = null;
  private String longName = null;
  private String description = null;
  private boolean hasArgument = false;
  private boolean optionalArgument = false;
  private boolean required = false;
  private String valueIfSet = null; // for parameters without argument
  private String valueIfNotSet = null; // for optional parameters that were not set
  private Integer numberOfArgs = null; // for parameters that receive more than one argument
  private INPUT_TYPE inputType = null;
  private CATEGORY_TYPE exportOptions = null;
  private FILE_FILTER_TYPE fileFilter = null;

  private HashMap<String, Option> options = new HashMap<String, Option>();

  public Parameter() {
  }

  /**
   * @return the parameter description
   */
  public String description() {
    return description;
  }

  /**
   * Set the parameter description that describes how or in which cases this
   * parameter is used
   * 
   * @param description
   *          the parameter description
   * @return This parameter, for method chaining.
   */
  public Parameter description(String description) {
    this.description = description;
    return this;
  }

  /**
   * @return true if this parameter takes an argument, false otherwise
   */
  public boolean hasArgument() {
    return hasArgument;
  }

  /**
   * Set the parameter to have/not have an argument.
   * 
   * @param hasArgument
   *          true if the parameter should receive an argument
   * @return This parameter, for method chaining.
   */
  public Parameter hasArgument(boolean hasArgument) {
    this.hasArgument = hasArgument;
    return this;
  }

  /**
   * If the parameter receives an obligatory/optional argument
   * 
   * @return true for optional argument; false for obligatory argument
   */
  public boolean isOptionalArgument() {
    return optionalArgument;
  }

  /**
   * Set the argument for this parameter as optional/obligatory.
   * 
   * @param optionalArgument
   *          true for optional argument; false for obligatory argument
   * @return This parameter, for method chaining.
   */
  public Parameter setOptionalArgument(boolean optionalArgument) {
    this.optionalArgument = optionalArgument;
    return this;
  }

  /**
   * Get the long name for this parameter. This does not include the import/export
   * prefix.
   * 
   * @return the long name for this parameter
   */
  public String longName() {
    return longName;
  }

  /**
   * Set the long name for this parameter. This name should only contain letters,
   * numbers and hyphens, but no check is made to ensure/enforce this policy.
   * 
   * Example: for a "Server name" parameter, this long name could be "server-name"
   * and that would generate the command line parameter "--import-server-name"
   * (for an import module) and "--export-server-name" (for an export module).
   * 
   * @param longName
   *          Set the long name for this parameter
   * @return This parameter, for method chaining.
   */
  public Parameter longName(String longName) {
    this.longName = longName;
    return this;
  }

  /**
   * @return true if this parameter is required
   */
  public boolean required() {
    return required;
  }

  /**
   * Set the parameter to be optional or mandatory
   * 
   * @param required
   *          true if the parameter is mandatory, false if it is optional
   * @return This parameter, for method chaining.
   */
  public Parameter required(boolean required) {
    this.required = required;
    return this;
  }

  /**
   * Get the short name for this parameter. This does not include the
   * import/export prefix.
   *
   * @return the long name for this parameter
   */
  public String shortName() {
    return shortName;
  }

  /**
   * Set the short name for this parameter. This name should only contain a few
   * letters but no check is made to ensure/enforce this policy.
   *
   * Example: for a "Server name" parameter, this short name could be "sn" and
   * that would generate the command line parameter "-isn" (for an import module)
   * and "-esn" (for an export module).
   * 
   * @param shortName
   *          Set the short name for this parameter
   * @return This parameter, for method chaining.
   */
  public Parameter shortName(String shortName) {
    this.shortName = shortName;
    return this;
  }

  /**
   * If this parameter is optional, then this value should be used when the
   * parameter is omitted
   * 
   * @return the default value to use when the parameter is not present
   */
  public String valueIfNotSet() {
    return valueIfNotSet;
  }

  /**
   * If this parameter is optional, then this value should be used when the
   * parameter is omitted.
   *
   * @param valueIfNotSet
   *          the default value to use when the parameter is not present
   * @return This parameter, for method chaining.
   */
  public Parameter valueIfNotSet(String valueIfNotSet) {
    this.valueIfNotSet = valueIfNotSet;
    return this;
  }

  /**
   * If the parameter has no arguments, then this should be the value associated
   * with the presence of the parameter
   * 
   * @return the parameter value when it is present but does not require arguments
   */
  public String valueIfSet() {
    return valueIfSet;
  }

  /**
   * If the parameter has no arguments, then this should be the value associated
   * with the presence of the parameter
   * 
   * @param valueIfSet
   *          value of the parameter when it is present
   * @return This parameter, for method chaining.
   */
  public Parameter valueIfSet(String valueIfSet) {
    this.valueIfSet = valueIfSet;
    return this;
  }

  /**
   * Gets the number of arguments for this parameter.
   *
   * @return the number of arguments for this parameter.
   */
  public Integer numberOfArgs() { return numberOfArgs; }

  /**
   * If the parameter has more than one argument, then this should be the value associated
   * with the number of arguments of the parameter
   *
   * @param numberOfArgs
   *          value of the parameter when it is present
   * @return This parameter, for method chaining.
   */
  public Parameter numberOfArgs(Integer numberOfArgs) {
    this.numberOfArgs = numberOfArgs;
    return this;
  }

  /**
   * Gets the input type for this parameter; Helper to automatize the DVBTK
   *
   * @return The type of input {@link INPUT_TYPE} enum.
   */
  public INPUT_TYPE getInputType() {
    return inputType;
  }

  /**
   *
   * @param type
   *          Set the type name for this parameter
   * @return This parameter, for method chaining.
   */
  public Parameter inputType(INPUT_TYPE type) {
    this.inputType = type;
    return this;
  }

  /**
   * Gets the export option type for this parameter; Helper to automatize the DVBTK
   *
   * @return The category type {@link CATEGORY_TYPE} enum.
   */
  public CATEGORY_TYPE getExportOptions() {
    return exportOptions;
  }

  /**
   *
   * @param exportOptions
   *          Set the export option name for this parameter
   * @return This parameter, for method chaining.
   */
  public Parameter exportOptions(CATEGORY_TYPE exportOptions) {
    this.exportOptions = exportOptions;
    return this;
  }

  public Parameter fileFilter(FILE_FILTER_TYPE fileFilter) {
    this.fileFilter = fileFilter;
    return this;
  }

  public FILE_FILTER_TYPE getFileFilter() { return fileFilter; }

  /**
   * Convert this parameter into a command line option (used internally by
   * CommonsCLI)
   * 
   * The Option object is saved after creation, so that subsequent calls to this
   * method using the same arguments do not return a new object and instead return
   * the previously returned object. Calls to this method using different
   * arguments produce different objects. This allows the comparison of objects
   * using ==
   *
   * @param shortNamePrefix
   *          the prefix to use for the command line option (usually "i" for
   *          import module parameters and "e" for export module parameters)
   * @param longNamePrefix
   *          the prefix to use for the command line option (usually "import" for
   *          import module parameters and "export" for export module parameters)
   * @return the option to be used by CommonsCLI
   */
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

  /**
   * Convert this parameter into a command line option (used internally by
   * CommonsCLI)
   *
   * The Option object is saved after creation, so that subsequent calls to this
   * method using the same arguments do not return a new object and instead return
   * the previously returned object. Calls to this method using different
   * arguments produce different objects. This allows the comparison of objects
   * using ==
   *
   * @return the option to be used by CommonsCLI
   */
  public Option toOption() {
    Option option = null;

    if (longName == null) {
      throw new RuntimeException("Parameter has no long name. All Parameter instances must have a long name.");
    }

    Option.Builder optionBuilder = Option.builder();

    if (shortName != null) {
      optionBuilder = Option.builder(shortName);
    }

    if (numberOfArgs == null) {
      option = optionBuilder.longOpt(longName).desc(description).hasArg(hasArgument)
          .required(required).optionalArg(optionalArgument).build();
    } else {
      option = optionBuilder.longOpt(longName).desc(description).hasArg(hasArgument)
          .required(required).optionalArg(optionalArgument).build();

      option.setArgs(Option.UNLIMITED_VALUES);
    }

    options.put(option.getLongOpt(), option);

    return option;
  }
}
