/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CLIEdit extends CLIHandler {

  private final ArrayList<EditModuleFactory> allEditFactories;

  private List<String> editModuleNames;
  private EditModuleFactory editModuleFactory;
  private Map<Parameter, List<String>> editModuleParameters;

  public CLIEdit(List<String> commandLineArguments, Collection<EditModuleFactory> editModuleFactories) {
    super(commandLineArguments);
    allEditFactories          = new ArrayList<>(editModuleFactories);
    editModuleNames           = new ArrayList<>();
  }

  /**
   * Gets the edit module parameters, obtained by parsing the
   * parameters
   *
   * @return The edit module configuration parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   */
  public Map<Parameter, List<String>> getEditModuleParameters() throws ParseException {
    if (editModuleFactory == null) {
      parse(commandLineArguments);
    }
    return editModuleParameters;
  }

  public EditModuleFactory getEditModuleFactory() throws ParseException {
    if (editModuleFactory == null) {
      parse(commandLineArguments);
    }
    return editModuleFactory;
  }

  private void parse(List<String> args) throws ParseException {
    EditModuleFactory factory = getEditFactory(args);

    editModuleParameters = getEditArguments(factory, args);
  }

  private HashMap<Parameter, List<String>> getEditArguments(EditModuleFactory factory, List<String> args) throws ParseException {
    // get appropriate command line options
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine;
    Options options = new Options();

    HashMap<String, Parameter> mapOptionToParameter = new HashMap<>();

    for (Parameter parameter : factory.getImportParameters().getParameters()) {
      Option option = parameter.toOption("i", "import");
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }

    for (ParameterGroup parameterGroup : factory.getImportParameters().getGroups()) {
      OptionGroup optionGroup = parameterGroup.toOptionGroup("i", "import");
      options.addOptionGroup(optionGroup);

      for (Parameter parameter : parameterGroup.getParameters()) {
        mapOptionToParameter.put(getUniqueOptionIdentifier(parameter.toOption("i", "import")), parameter);
      }
    }

    for (Parameter parameter: factory.getParameters().getParameters()) {
      Option option = parameter.toOption();
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }

    // parse the command line arguments with those options
    try {
      commandLine = commandLineParser.parse(options, args.toArray(new String[] {}), false);
      if (!commandLine.getArgList().isEmpty()) {
        throw new ParseException("Unrecognized option: " + commandLine.getArgList().get(0));
      }
    } catch (MissingOptionException e) {
      // use long names instead of short names in the error message
      List<String> missingShort = e.getMissingOptions();
      List<String> missingLong = new ArrayList<String>();
      for (String shortOption : missingShort) {
        missingLong.add(options.getOption(shortOption).getLongOpt());
      }
      LOGGER.debug("MissingOptionException (the original, unmodified exception)", e);
      throw new MissingOptionException(missingLong);
    } catch (MissingArgumentException e) {
      // use long names instead of short names in the error message
      Option faulty = e.getOption();
      LOGGER.debug("MissingArgumentException (the original, unmodified exception)", e);
      throw new MissingArgumentException("Missing the argument for the set: " + faulty.getValue(0));
    }

    // create arguments to pass to factory
    HashMap<Parameter, List<String>> editModuleArguments = new HashMap<>();

    for (Option option : commandLine.getOptions()) {
      Parameter p = mapOptionToParameter.get(getUniqueOptionIdentifier(option));
      if (p != null) {
        if (isImportOption(option)) {
          if (p.hasArgument()) {
            ArrayList<String> values = updateArgs(editModuleArguments, p, option.getValue(p.valueIfNotSet()));
            editModuleArguments.put(p, values);
          } else {
            ArrayList<String> values = updateArgs(editModuleArguments, p, option.getValue(p.valueIfSet()));
            editModuleArguments.put(p, values);
          }
        } else if (isSetFieldOption(option)) {
          if (p.hasArgument()) {
            StringBuilder sb = new StringBuilder();

            for (String s : option.getValues()) {
              sb.append(s).append(Constants.SEPARATOR);
            }
            ArrayList<String> values = updateArgs(editModuleArguments, p, sb.toString());
            editModuleArguments.put(p,values);
          }
        } else if(isListOption(option)) {
          if (p.hasArgument()) {

          } else {
            ArrayList<String> values = updateArgs(editModuleArguments, p, option.getValue(p.valueIfNotSet()));
            editModuleArguments.put(p, values);
          }

        } else {
          throw new ParseException("Unexpected parse exception occurred.");
        }
      }
    }
    return editModuleArguments;
  }

  private ArrayList<String> updateArgs(HashMap<Parameter, List<String>> arguments, Parameter parameter, String value) {
    ArrayList<String> values = new ArrayList<>();

    if (arguments.get(parameter) != null) {
      values = new ArrayList<>(arguments.get(parameter));
    }

    values.add(value);

    return values;
  }

  private EditModuleFactory getEditFactory(List<String> args) throws ParseException {
    for (EditModuleFactory factory : allEditFactories) {
      String moduleName = factory.getModuleName();
      if (moduleName.equalsIgnoreCase("edit-siard") && factory.isEnabled()) {
        editModuleFactory = factory;
      }
    }

    if (editModuleFactory == null) {
      throw new ParseException("Invalid edit module.");
    }

    return editModuleFactory;
  }

  private static boolean isImportOption(Option option) {
    final String type = "i";
    if (StringUtils.isNotBlank(option.getOpt())) {
      return option.getOpt().startsWith(type);
    } else if (StringUtils.isNotBlank(option.getLongOpt())) {
      return option.getLongOpt().startsWith(type);
    }
    return false;
  }

  private static boolean isSetFieldOption(Option option) {
    final String typeShot = "s";
    final String typeLong = "set";
    if (StringUtils.isNotBlank(option.getOpt())) {
      return option.getOpt().contentEquals(typeShot);
    } else if (StringUtils.isNotBlank(option.getLongOpt())) {
      return option.getLongOpt().contentEquals(typeLong);
    }
    return false;
  }

  private static boolean isListOption(Option option) {
    final String typeShot = "l";
    final String typeLong = "list";
    if (StringUtils.isNotBlank(option.getOpt())) {
      return option.getOpt().contentEquals(typeShot);
    } else if (StringUtils.isNotBlank(option.getLongOpt())) {
      return option.getLongOpt().contentEquals(typeLong);
    }
    return false;
  }
}
