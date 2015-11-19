package com.databasepreservation.cli;

import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.naming.OperationNotSupportedException;

import net.xeoh.plugins.base.PluginManager;
import net.xeoh.plugins.base.impl.PluginManagerFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.Main;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
import com.databasepreservation.model.parameters.Parameters;

/**
 * Handles command line interface
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CLI {
  private static final CustomLogger logger = CustomLogger.getLogger(CLI.class);

  private final ArrayList<DatabaseModuleFactory> factories;
  private final List<String> commandLineArguments;
  private DatabaseImportModule importModule;
  private String importModuleName;
  private String exportModuleName;
  private DatabaseExportModule exportModule;

  public CLI(List<String> commandLineArguments, List<Class<? extends DatabaseModuleFactory>> databaseModuleFactories) {
    factories = new ArrayList<DatabaseModuleFactory>();
    this.commandLineArguments = commandLineArguments;
    try {
      for (Class<? extends DatabaseModuleFactory> factoryClass : databaseModuleFactories) {
        factories.add(factoryClass.newInstance());
      }
    } catch (InstantiationException e) {
      logger.error("Error initializing CLI", e);
    } catch (IllegalAccessException e) {
      logger.error("Error initializing CLI", e);
    }
    includePluginModules();
  }

  public CLI(List<String> commandLineArguments, DatabaseModuleFactory... databaseModuleFactories) {
    factories = new ArrayList<DatabaseModuleFactory>(Arrays.asList(databaseModuleFactories));
    this.commandLineArguments = commandLineArguments;
    includePluginModules();
  }

  private void includePluginModules() {
    // find plugins in command line arguments
    String pluginString = null;
    Iterator<String> argsIterator = commandLineArguments.iterator();
    while (argsIterator.hasNext()) {
      String arg = argsIterator.next();
      if (arg.equals("-p") || arg.equals("--plugin")) {
        pluginString = argsIterator.next();
        break;
      } else if (StringUtils.startsWith(arg, "--plugin=")) {
        pluginString = arg.substring(9); // 9 is the size of the string
                                         // "--plugin="
        break;
      }
    }

    if (pluginString != null) {
      for (String plugin : pluginString.split(";")) {
        PluginManager pm = PluginManagerFactory.createPluginManager();
        try {
          URI pluginURI = new URI(plugin);
          if (pluginURI.getScheme() == null) {
            pluginURI = new URI("file://" + plugin);
          }
          pm.addPluginsFrom(pluginURI);
        } catch (URISyntaxException e) {
          logger.warn("Plugin not found: " + plugin);
        }
        factories.add(pm.getPlugin(DatabaseModuleFactory.class));
      }
    }
  }

  public DatabaseImportModule getImportModule() throws ParseException, LicenseNotAcceptedException {
    if (importModule == null) {
      parse(commandLineArguments);
    }
    return importModule;
  }

  public DatabaseExportModule getExportModule() throws ParseException, LicenseNotAcceptedException {
    if (exportModule == null) {
      parse(commandLineArguments);
    }
    return exportModule;
  }

  public String getExportModuleName() {
    return exportModuleName;
  }

  public String getImportModuleName() {
    return importModuleName;
  }

  public void printHelp() {
    printHelp(System.out);
  }

  /**
   * Parses the argument list and creates new import and export modules
   *
   * @param args
   *          The command line arguments
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private void parse(List<String> args) throws ParseException, LicenseNotAcceptedException {
    DatabaseModuleFactoriesPair databaseModuleFactoriesPair = getModuleFactories(args);

    try {
      importModuleName = databaseModuleFactoriesPair.getImportModuleFactory().getModuleName();
      exportModuleName = databaseModuleFactoriesPair.getExportModuleFactory().getModuleName();
      DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = getModuleArguments(
        databaseModuleFactoriesPair, args);

      // set import and export modules
      importModule = databaseModuleFactoriesPair.getImportModuleFactory().buildImportModule(
        databaseModuleFactoriesArguments.getImportModuleArguments());
      exportModule = databaseModuleFactoriesPair.getExportModuleFactory().buildExportModule(
        databaseModuleFactoriesArguments.getExportModuleArguments());
    } catch (OperationNotSupportedException e) {
      throw new ParseException("Module does not support the requested mode.");
    }
  }

  /**
   * Given the arguments, determines the DatabaseModuleFactory objects that
   * should be used to create the import and export modules
   *
   * @param args
   *          The command line arguments
   * @return A pair of DatabaseModuleFactory objects containing the selected
   *         import and export module factories
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private DatabaseModuleFactoriesPair getModuleFactories(List<String> args) throws ParseException {
    // check if args contains exactly one import and one export module
    String importModuleName = null;
    String exportModuleName = null;
    int importModulesFound = 0;
    int exportModulesFound = 0;
    Iterator<String> argsIterator = args.iterator();
    try {
      while (argsIterator.hasNext()) {
        String arg = argsIterator.next();
        if (arg.equals("-i") || arg.equals("--import")) {
          importModuleName = argsIterator.next();
          importModulesFound++;
        } else if (arg.equals("-e") || arg.equals("--export")) {
          exportModuleName = argsIterator.next();
          exportModulesFound++;
        } else if (StringUtils.startsWith(arg, "--import=")) {
          importModuleName = arg.substring(9); // 9 is the size of the string
                                               // "--import="
          importModulesFound++;
        } else if (StringUtils.startsWith(arg, "--export=")) {
          exportModuleName = arg.substring(9); // 9 is the size of the string
                                               // "--export="
          exportModulesFound++;
        }
      }
    } catch (NoSuchElementException e) {
      throw new ParseException("Missing module name.");
    }
    if (importModulesFound != 1 || exportModulesFound != 1) {
      throw new ParseException("Exactly one import module and one export module must be specified.");
    }

    // check if both module names correspond to real module names
    DatabaseModuleFactory importModuleFactory = null;
    DatabaseModuleFactory exportModuleFactory = null;
    for (DatabaseModuleFactory factory : factories) {
      String moduleName = factory.getModuleName();
      if (moduleName.equalsIgnoreCase(importModuleName) && factory.producesImportModules()) {
        importModuleFactory = factory;
      }
      if (moduleName.equalsIgnoreCase(exportModuleName) && factory.producesExportModules()) {
        exportModuleFactory = factory;
      }
    }
    if (importModuleFactory == null) {
      throw new ParseException("Invalid import module.");
    } else if (exportModuleFactory == null) {
      throw new ParseException("Invalid export module.");
    }
    return new DatabaseModuleFactoriesPair(importModuleFactory, exportModuleFactory);
  }

  /**
   * Obtains the arguments needed to create new import and export modules
   *
   * @param factoriesPair
   *          A pair of DatabaseModuleFactory objects containing the selected
   *          import and export module factories
   * @param args
   *          The command line arguments
   * @return A DatabaseModuleFactoriesArguments containing the arguments to
   *         create the import and export modules
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private DatabaseModuleFactoriesArguments getModuleArguments(DatabaseModuleFactoriesPair factoriesPair,
    List<String> args) throws ParseException, OperationNotSupportedException {
    DatabaseModuleFactory importModuleFactory = factoriesPair.getImportModuleFactory();
    DatabaseModuleFactory exportModuleFactory = factoriesPair.getExportModuleFactory();

    // get appropriate command line options
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine;
    Options options = new Options();

    HashMap<String, Parameter> mapOptionToParameter = new HashMap<String, Parameter>();

    for (Parameter parameter : importModuleFactory.getImportModuleParameters().getParameters()) {
      Option option = parameter.toOption("i", "import");
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }
    for (ParameterGroup parameterGroup : importModuleFactory.getImportModuleParameters().getGroups()) {
      OptionGroup optionGroup = parameterGroup.toOptionGroup("i", "import");
      options.addOptionGroup(optionGroup);

      for (Parameter parameter : parameterGroup.getParameters()) {
        mapOptionToParameter.put(getUniqueOptionIdentifier(parameter.toOption("i", "import")), parameter);
      }
    }
    for (Parameter parameter : exportModuleFactory.getExportModuleParameters().getParameters()) {
      Option option = parameter.toOption("e", "export");
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }
    for (ParameterGroup parameterGroup : exportModuleFactory.getExportModuleParameters().getGroups()) {
      OptionGroup optionGroup = parameterGroup.toOptionGroup("e", "export");
      options.addOptionGroup(optionGroup);

      for (Parameter parameter : parameterGroup.getParameters()) {
        mapOptionToParameter.put(getUniqueOptionIdentifier(parameter.toOption("e", "export")), parameter);
      }
    }

    Option importOption = Option.builder("i").longOpt("import").hasArg().optionalArg(false).build();
    Option exportOption = Option.builder("e").longOpt("export").hasArg().optionalArg(false).build();
    Option pluginOption = Option.builder("p").longOpt("plugin").hasArg().optionalArg(false).build();
    options.addOption(importOption);
    options.addOption(exportOption);
    options.addOption(pluginOption);

    // new HelpFormatter().printHelp(80, "dbptk", "\nModule Options:", options,
    // null, true);

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
      throw new MissingOptionException(missingLong);
    }

    // create arguments to pass to factory
    HashMap<Parameter, String> importModuleArguments = new HashMap<Parameter, String>();
    HashMap<Parameter, String> exportModuleArguments = new HashMap<Parameter, String>();
    for (Option option : commandLine.getOptions()) {
      Parameter p = mapOptionToParameter.get(getUniqueOptionIdentifier(option));
      if (p != null) {
        if (isImportModuleOption(option)) {
          if (p.hasArgument()) {
            importModuleArguments.put(p, option.getValue(p.valueIfNotSet()));
          } else {
            importModuleArguments.put(p, p.valueIfSet());
          }
        } else if (isExportModuleOption(option)) {
          if (p.hasArgument()) {
            exportModuleArguments.put(p, option.getValue(p.valueIfNotSet()));
          } else {
            exportModuleArguments.put(p, p.valueIfSet());
          }
        } else {
          throw new ParseException("Unexpected parse exception occurred.");
        }
      }
    }
    return new DatabaseModuleFactoriesArguments(importModuleArguments, exportModuleArguments);
  }

  private void printHelp(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    out
      .append("Database Preservation Toolkit")
      .append(getApplicationVersion())
      .append("\nMore info: http://www.database-preservation.com")
      .append("\n")
      .append("Usage: dbptk [plugin] <importModule> [import module options] <exportModule> [export module options]\n\n");

    ArrayList<DatabaseModuleFactory> modulesList = new ArrayList<DatabaseModuleFactory>(factories);
    Collections.sort(modulesList, new DatabaseModuleFactoryNameComparator());
    out.append("## Plugin:\n");
    out
      .append("    -p, --plugin=plugin.jar    (optional) the file containing a plugin module. Several plugins can be specified, separated by a semi-colon (;)\n");

    out.append("\n## Available import modules: -i <module>, --import=module\n");
    for (DatabaseModuleFactory factory : modulesList) {
      if (factory.producesImportModules()) {
        try {
          out.append(printModuleHelp("Import module: " + factory.getModuleName(), "i", "import",
            factory.getImportModuleParameters()));
        } catch (OperationNotSupportedException e) {
          // this should never happen
        }
      }
    }

    out.append("\n## Available export modules: -e <module>, --export=module\n");
    for (DatabaseModuleFactory factory : modulesList) {
      if (factory.producesExportModules()) {
        try {
          out.append(printModuleHelp("Export module: " + factory.getModuleName(), "e", "export",
            factory.getExportModuleParameters()));
        } catch (OperationNotSupportedException e) {
          // this should never happen
        }
      }
    }

    printStream.append(out).flush();
  }

  private String printModuleHelp(String moduleDesignation, String shortParameterPrefix, String longParameterPrefix,
    Parameters moduleParameters) {
    StringBuilder out = new StringBuilder();

    String space = "    ";

    out.append("\n").append(moduleDesignation);

    for (Parameter parameter : moduleParameters.getParameters()) {
      out.append(printParameterHelp(space, shortParameterPrefix, longParameterPrefix, parameter));
    }

    for (ParameterGroup parameterGroup : moduleParameters.getGroups()) {
      for (Parameter parameter : parameterGroup.getParameters()) {
        out.append(printParameterHelp(space, shortParameterPrefix, longParameterPrefix, parameter));
      }
    }
    out.append("\n");

    return out.toString();
  }

  private String printParameterHelp(String space, String shortPrefix, String longPrefix, Parameter parameter) {
    StringBuilder out = new StringBuilder();

    out.append("\n").append(space);

    if (StringUtils.isNotBlank(parameter.shortName())) {
      out.append("-").append(shortPrefix).append(parameter.shortName()).append(", ");
    }

    out.append("--").append(longPrefix).append("-").append(parameter.longName());

    if (parameter.hasArgument()) {
      if (parameter.isOptionalArgument()) {
        out.append("[");
      }
      out.append("=value");
      if (parameter.isOptionalArgument()) {
        out.append("]");
      }
    }

    out.append(space);
    if (parameter.required()) {
      out.append("(required) ");
    } else {
      out.append("(optional) ");
    }
    out.append(parameter.description());

    return out.toString();
  }

  public static String getApplicationVersion() {
    if (Main.APP_VERSION != null) {
      return ", v" + Main.APP_VERSION;
    } else {
      return ""; // omit version if it is not known
    }
  }

  public void printLicense(String license) {
    System.err.println(license);
  }

  private static class DatabaseModuleFactoryNameComparator implements Comparator<DatabaseModuleFactory> {
    @Override
    public int compare(DatabaseModuleFactory o1, DatabaseModuleFactory o2) {
      return o1.getModuleName().compareTo(o2.getModuleName());
    }
  }

  private static String getUniqueOptionIdentifier(Option option) {
    final String delimiter = "\r\f\n"; // some string that should never occur in
                                       // option shortName nor longName
    return new StringBuilder().append(delimiter).append(option.getOpt()).append(delimiter).append(option.getLongOpt())
      .append(delimiter).toString();
  }

  private static boolean isImportModuleOption(Option option) {
    final String type = "i";
    if (StringUtils.isNotBlank(option.getOpt())) {
      return option.getOpt().startsWith(type);
    } else if (StringUtils.isNotBlank(option.getLongOpt())) {
      return option.getLongOpt().startsWith(type);
    }
    return false;
  }

  private static boolean isExportModuleOption(Option option) {
    final String type = "e";
    if (StringUtils.isNotBlank(option.getOpt())) {
      return option.getOpt().startsWith(type);
    } else if (StringUtils.isNotBlank(option.getLongOpt())) {
      return option.getLongOpt().startsWith(type);
    }
    return false;
  }

  /**
   * Pair containing the import and export module factories
   */
  public class DatabaseModuleFactoriesPair {
    // left: import, right: export
    private final ImmutablePair<DatabaseModuleFactory, DatabaseModuleFactory> factories;

    public DatabaseModuleFactoriesPair(DatabaseModuleFactory importModuleFactory,
      DatabaseModuleFactory exportModuleFactory) {
      factories = new ImmutablePair<DatabaseModuleFactory, DatabaseModuleFactory>(importModuleFactory,
        exportModuleFactory);
    }

    public DatabaseModuleFactory getImportModuleFactory() {
      return factories.getLeft();
    }

    public DatabaseModuleFactory getExportModuleFactory() {
      return factories.getRight();
    }
  }

  /**
   * Pair containing the arguments to create the import and export modules
   */
  public class DatabaseModuleFactoriesArguments {
    // left: import, right: export
    private final ImmutablePair<HashMap<Parameter, String>, HashMap<Parameter, String>> factories;

    public DatabaseModuleFactoriesArguments(HashMap<Parameter, String> importModuleArguments,
      HashMap<Parameter, String> exportModuleArguments) {
      factories = new ImmutablePair<HashMap<Parameter, String>, HashMap<Parameter, String>>(importModuleArguments,
        exportModuleArguments);
    }

    public HashMap<Parameter, String> getImportModuleArguments() {
      return factories.getLeft();
    }

    public HashMap<Parameter, String> getExportModuleArguments() {
      return factories.getRight();
    }
  }
}
