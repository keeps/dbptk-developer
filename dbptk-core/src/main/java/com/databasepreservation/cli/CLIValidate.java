package com.databasepreservation.cli;

import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CLIValidate extends CLIHandler {
  private final ArrayList<ValidateModuleFactory> allValidateFactories;
  private ValidateModuleFactory validateModuleFactory;
  private Map<Parameter, String> validateModuleParameters;
  private String siardPackage;

  public CLIValidate(List<String> commandLineArguments, Collection<ValidateModuleFactory> validateModuleFactories) {
    super(commandLineArguments);
    allValidateFactories = new ArrayList<>(validateModuleFactories);
  }

  /**
   * Gets the path to the SIARD archive
   *
   * @return The path to the SIARD archive
   */
  public String getSIARDPackage() {
    return siardPackage;
  }

  /**
   * Gets the validate module parameters, obtained by parsing the parameters
   *
   * @return The validate module configuration parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   */
  public Map<Parameter, String> getValidateModuleParameters() throws ParseException {
    if (validateModuleFactory == null) {
      parse(commandLineArguments);
    }
    return validateModuleParameters;
  }

  /**
   * Gets the validate module factory, obtained by parsing the parameters
   *
   * @return The validate module configuration parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   */
  public ValidateModuleFactory getValidateModuleFactory() throws ParseException {
    if (validateModuleFactory == null) {
      parse(commandLineArguments);
    }
    return validateModuleFactory;
  }

  // Auxiliary Internal Methods

  private void parse(List<String> args) throws ParseException {
    ValidateModuleFactory factory = getValidateFactory(args);

    validateModuleParameters = getValidationArguments(factory, args);
  }

  private HashMap<Parameter, String> getValidationArguments(ValidateModuleFactory factory, List<String> args)
      throws ParseException {
    // get appropriate command line options
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine;
    Options options = new Options();

    HashMap<String, Parameter> mapOptionToParameter = new HashMap<>();

    for (Parameter parameter : factory.getParameters().getParameters()) {
      Option option = parameter.toOption("i", "import");
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }

    Option importOption = Option.builder("i").longOpt("import").hasArg().optionalArg(false).build();
    options.addOption(importOption);

    commandLine = commandLineParse(commandLineParser, options, args);

    // create arguments to pass to factory
    HashMap<Parameter, String> validateModuleArguments = new HashMap<>();

    for (Option option : commandLine.getOptions()) {
      Parameter p = mapOptionToParameter.get(getUniqueOptionIdentifier(option));
      if (p != null) {
          if (p.hasArgument()) {
            if (p.longName().contentEquals("file")) {
              siardPackage = option.getValue(p.valueIfNotSet());
              validateModuleArguments.put(p, option.getValue(p.valueIfNotSet()));
            }
          }
        } else {
          throw new ParseException("Unexpected parse exception occurred.");
        }
      }

    return validateModuleArguments;
  }

  private ValidateModuleFactory getValidateFactory(List<String> args) throws ParseException {
    for (ValidateModuleFactory factory : allValidateFactories) {
      String moduleName = factory.getModuleName();
      if (moduleName.equalsIgnoreCase("validate-siard") && factory.isEnabled()) {
        validateModuleFactory = factory;
      }
    }

    if (validateModuleFactory == null) {
      throw new ParseException("Invalid validation module.");
    }

    return validateModuleFactory;
  }
}
