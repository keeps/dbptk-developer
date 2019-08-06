/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CLIMigrate extends CLIHandler {

  private final ArrayList<DatabaseModuleFactory> allModuleFactories;
  private final ArrayList<DatabaseFilterFactory> allFilterFactories;

  private String importModuleName;
  private DatabaseModuleFactory importModuleFactory;
  private Map<Parameter, String> importModuleParameters;

  private String exportModuleName;
  private DatabaseModuleFactory exportModuleFactory;
  private Map<Parameter, String> exportModuleParameters;

  private List<String> filterNames;
  private List<DatabaseFilterFactory> filterFactories;
  private List<Map<Parameter, String>> filterParameters;

  private boolean forceDisableEncryption = false;

  /**
   * Create a new CLI Migrate handler
   *
   * @param commandLineArguments
   *          List of command line parameters as they are received by Main.main
   * @param databaseModuleFactories
   *          List of available module factories
   */
  public CLIMigrate(List<String> commandLineArguments, Collection<DatabaseModuleFactory> databaseModuleFactories) {
    super(commandLineArguments);
    allModuleFactories        = new ArrayList<>(databaseModuleFactories);
    allFilterFactories        = new ArrayList<>();
    filterNames               = new ArrayList<>();
  }

  public CLIMigrate(List<String> commandLineArguments, Collection<DatabaseModuleFactory> databaseModuleFactories,
             Collection<DatabaseFilterFactory> databaseFilterFactories) {
    super(commandLineArguments);
    allModuleFactories        = new ArrayList<>(databaseModuleFactories);
    allFilterFactories        = new ArrayList<>(databaseFilterFactories);
    filterNames               = new ArrayList<>();
  }

  /**
   * Create a new CLI handler
   *
   * @param commandLineArguments
   *          List of command line parameters as they are received by Main.main
   * @param databaseModuleFactories
   *          Array of module factories
   */
  public CLIMigrate(List<String> commandLineArguments, DatabaseModuleFactory... databaseModuleFactories) {
    this(commandLineArguments, Arrays.asList(databaseModuleFactories));
  }

  /**
   * Gets the database import module parameters, obtained by parsing the
   * parameters
   *
   * @return The import module configuration parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   * @throws LicenseNotAcceptedException
   *           if the license for using a module was not accepted
   */
  public Map<Parameter, String> getImportModuleParameters() throws ParseException {
    if (importModuleFactory == null) {
      parse(commandLineArguments);
    }
    return importModuleParameters;
  }

  /**
   * Gets the database import module factory, obtained by parsing the parameters
   *
   * @return The database module factory capable of producing the import module
   *         specified in the parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   * @throws LicenseNotAcceptedException
   *           if the license for using a module was not accepted
   */
  public DatabaseModuleFactory getImportModuleFactory() throws ParseException {
    if (importModuleFactory == null) {
      parse(commandLineArguments);
    }
    return importModuleFactory;
  }

  /**
   * Gets the database export module parameters, obtained by parsing the
   * parameters
   *
   * @return The export module configuration parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   * @throws LicenseNotAcceptedException
   *           if the license for using a module was not accepted
   */
  public Map<Parameter, String> getExportModuleParameters() throws ParseException {
    if (exportModuleFactory == null) {
      parse(commandLineArguments);
    }
    return exportModuleParameters;
  }

  /**
   * Gets the database export module factory, obtained by parsing the parameters
   *
   * @return The database module factory capable of producing the export module
   *         specified in the parameters
   * @throws ParseException
   *           if there was an error parsing the command line parameters
   * @throws LicenseNotAcceptedException
   *           if the license for using a module was not accepted
   */
  public DatabaseModuleFactory getExportModuleFactory() throws ParseException {
    if (exportModuleFactory == null) {
      parse(commandLineArguments);
    }
    return exportModuleFactory;
  }

  /**
   * Gets the name of the export module. Note that this method does not trigger
   * the lazy loading mechanism for parsing the parameters, so the value may be
   * null if no calls to getImportModule() or getExportModule() were made.
   *
   * @return The export module name. null if the command line parameters have not
   *         been parsed yet
   */
  public String getExportModuleName() {
    return exportModuleName;
  }

  /**
   * Gets the name of the import module. Note that this method does not trigger
   * the lazy loading mechanism for parsing the parameters, so the value may be
   * null if no calls to getImportModule() or getExportModule() were made.
   *
   * @return The import module name. null if the command line parameters have not
   *         been parsed yet
   */
  public String getImportModuleName() { return importModuleName; }

  public List<String> getFilterNames() { return filterNames; }

  public List<DatabaseFilterFactory> getFilterFactories() {
    return filterFactories;
  }

  public List<Map<Parameter, String>> getFilterParameters() {
    return filterParameters;
  }

  /**
   * Discards import and export module instances and disables encryption. Next
   * time #parse is run, encryption will be disabled for modules that support that
   * option.
   */
  public void disableEncryption() {
    forceDisableEncryption = true;
    importModuleFactory = null;
    exportModuleFactory = null;
  }

  /**
   * Parses the argument list and creates new import and export modules
   *
   * @param args
   *          The command line arguments
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private void parse(List<String> args) throws ParseException {
    DatabaseModuleFactories databaseModuleFactories = getModuleFactories(args);

    importModuleFactory = databaseModuleFactories.getImportModuleFactory();
    exportModuleFactory = databaseModuleFactories.getExportModuleFactory();
    filterFactories = databaseModuleFactories.getFilterFactories();

    try {
      importModuleName = importModuleFactory.getModuleName();
      exportModuleName = exportModuleFactory.getModuleName();

      for (DatabaseFilterFactory factory : filterFactories) {
        filterNames.add(factory.getFilterName());
      }

      DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = getModuleArguments(
          databaseModuleFactories, args);

      if (forceDisableEncryption) {
        // inject disable encryption for import module
        for (Parameter parameter : importModuleFactory.getImportModuleParameters().getParameters()) {
          if (parameter.longName().equalsIgnoreCase("disable-encryption")) {
            if (!databaseModuleFactoriesArguments.getImportModuleArguments().containsKey(parameter)) {
              databaseModuleFactoriesArguments.getImportModuleArguments().put(parameter, "true");
            }
            break;
          }
        }

        // inject disable encryption for export module
        for (Parameter parameter : exportModuleFactory.getExportModuleParameters().getParameters()) {
          if (parameter.longName().equalsIgnoreCase("disable-encryption")) {
            if (!databaseModuleFactoriesArguments.getExportModuleArguments().containsKey(parameter)) {
              databaseModuleFactoriesArguments.getExportModuleArguments().put(parameter, "true");
            }
            break;
          }
        }
      }

      importModuleParameters = databaseModuleFactoriesArguments.getImportModuleArguments();
      exportModuleParameters = databaseModuleFactoriesArguments.getExportModuleArguments();
      filterParameters = databaseModuleFactoriesArguments.getFilterModulesArguments();

    } catch (UnsupportedModuleException e) {
      LOGGER.debug("UnsupportedModuleException", e);
      throw new ParseException("Module does not support the requested mode.");
    }
  }

  /**
   * Given the arguments, determines the DatabaseModuleFactory objects that should
   * be used to create the import and export modules
   *
   * @param args
   *          The command line arguments
   * @return A pair of DatabaseModuleFactory objects containing the selected
   *         import and export module factories
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private DatabaseModuleFactories getModuleFactories(List<String> args) throws ParseException {
    // check if args contains exactly one import and one export module
    String importModuleName = null;
    String exportModuleName = null;
    List<String> filterModuleNames = new ArrayList<>();

    int importModulesFound = 0;
    int exportModulesFound = 0;

    Iterator<String> argsIterator = args.iterator();
    try {
      while (argsIterator.hasNext()) {
        String arg = argsIterator.next();
        if ("-i".equals(arg) || "--import".equals(arg)) {
          importModuleName = argsIterator.next();
          importModulesFound++;
        } else if ("-e".equals(arg) || "--export".equals(arg)) {
          exportModuleName = argsIterator.next();
          exportModulesFound++;
        } else if (StringUtils.startsWith(arg, "--import=")) {
          // 9 is the size of the string "--import="
          importModuleName = arg.substring(9);
          importModulesFound++;
        } else if (StringUtils.startsWith(arg, "--export=")) {
          // 9 is the size of the string "--export="
          exportModuleName = arg.substring(9);
          exportModulesFound++;
        } else if ("-f".equals(arg) || "--filter".equals(arg) || "--filters".equals(arg)) {
          String filterNames = argsIterator.next();
          filterModuleNames = Arrays.asList(filterNames.split(","));
        } else if (StringUtils.startsWith(arg, "--filter=")) {
          filterModuleNames = Arrays.asList(arg.substring(9).split(","));
        } else if (StringUtils.startsWith(arg, "--filters=")) {
          filterModuleNames = Arrays.asList(arg.substring(10).split(","));
        }
      }
    } catch (NoSuchElementException e) {
      LOGGER.debug("NoSuchElementException", e);
      throw new ParseException("Missing module name.");
    }

    if (importModulesFound != 1 || exportModulesFound != 1) {
      throw new ParseException("Exactly one import module and one export module must be specified.");
    }

    // check if both module names correspond to real module names
    DatabaseModuleFactory importModuleFactory = null;
    DatabaseModuleFactory exportModuleFactory = null;
    List<DatabaseFilterFactory> filterModuleFactories = new ArrayList<>();
    for (DatabaseModuleFactory factory : allModuleFactories) {
      String moduleName = factory.getModuleName();
      if (moduleName.equalsIgnoreCase(importModuleName) && factory.producesImportModules()) {
        importModuleFactory = factory;
      }
      if (moduleName.equalsIgnoreCase(exportModuleName) && factory.producesExportModules()) {
        exportModuleFactory = factory;
      }
    }

    // check if filter names correspond to real filter module names
    for (String filterModuleName : filterModuleNames) {
      boolean isRealFilterName = false;
      for (DatabaseFilterFactory factory : allFilterFactories) {
        String filterName = factory.getFilterName();
        if (filterName.equalsIgnoreCase(filterModuleName)) {
          filterModuleFactories.add(factory);
          isRealFilterName = true;
          break;
        }
      }
      if (!isRealFilterName) {
        throw new ParseException("Invalid filter module '" + filterModuleName + "'.");
      }
    }

    if (importModuleFactory == null) {
      throw new ParseException("Invalid import module.");
    } else if (exportModuleFactory == null) {
      throw new ParseException("Invalid export module.");
    }
    return new DatabaseModuleFactories(importModuleFactory, exportModuleFactory, filterModuleFactories);
  }

  /**
   * Obtains the arguments needed to create new import and export modules
   *
   * @param factories
   *          A pair of DatabaseModuleFactory objects containing the selected
   *          import and export module factories
   * @param args
   *          The command line arguments
   * @return A DatabaseModuleFactoriesArguments containing the arguments to create
   *         the import and export modules
   * @throws ParseException
   *           If the arguments could not be parsed or are invalid
   */
  private DatabaseModuleFactoriesArguments getModuleArguments(DatabaseModuleFactories factories, List<String> args)
      throws ParseException, UnsupportedModuleException {
    DatabaseModuleFactory importModuleFactory = factories.getImportModuleFactory();
    DatabaseModuleFactory exportModuleFactory = factories.getExportModuleFactory();
    List<DatabaseFilterFactory> filterFactories = factories.getFilterFactories();

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

    for (int i = 0; i < filterFactories.size(); i++) {
      for (Parameter parameter : filterFactories.get(i).getParameters().getParameters()) {
        Option option = parameter.toOption("f" + (i+1), "filter" + (i+1));
        options.addOption(option);
        mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
      }
    }

    Option importOption = Option.builder("i").longOpt("import").hasArg().optionalArg(false).build();
    Option exportOption = Option.builder("e").longOpt("export").hasArg().optionalArg(false).build();
    Option filtersOption = Option.builder("f").longOpt("filter").hasArg().optionalArg(false).required(false).build();
    options.addOption(importOption);
    options.addOption(exportOption);
    options.addOption(filtersOption);

    // new HelpFormatter().printHelp(80, "dbptk", "\nModule Options:", options,
    // null, true);

    commandLine = commandLineParse(commandLineParser, options, args);

    // create arguments to pass to factory
    HashMap<Parameter, String> importModuleArguments = new HashMap<>();
    HashMap<Parameter, String> exportModuleArguments = new HashMap<>();
    List<Map<Parameter, String>> filterModuleArguments = new ArrayList<>();
    for (int i = 0; i < filterFactories.size(); i++) {
      filterModuleArguments.add(new HashMap<Parameter, String>());
    }

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
        } else if (isFilterModuleOption(option)) {
          // find out which filter the option refers to
          int filterIndex = new Scanner(option.getOpt()).useDelimiter("\\D+").nextInt();

          if (filterIndex > filterModuleArguments.size()) {
            throw new ParseException(
                "No filter matching index given '" + filterIndex + "' in parameter '" + option.getOpt() + "'.");
          } else {
            if (p.hasArgument()) {
              filterModuleArguments.get(filterIndex - 1).put(p, option.getValue(p.valueIfNotSet()));
            } else {
              filterModuleArguments.get(filterIndex - 1).put(p, p.valueIfSet());
            }
          }
        } else {
          throw new ParseException("Unexpected parse exception occurred.");
        }
      }
    }
    return new DatabaseModuleFactoriesArguments(importModuleArguments, exportModuleArguments, filterModuleArguments);
  }

  protected static class DatabaseModuleFactoryNameComparator implements Comparator<DatabaseModuleFactory> {
    @Override
    public int compare(DatabaseModuleFactory o1, DatabaseModuleFactory o2) {
      return o1.getModuleName().compareTo(o2.getModuleName());
    }
  }

  protected static class DatabaseFilterFactoryNameComparator implements Comparator<DatabaseFilterFactory> {
    @Override
    public int compare(DatabaseFilterFactory o1, DatabaseFilterFactory o2) {
      return o1.getFilterName().compareTo(o2.getFilterName());
    }
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

  private static boolean isFilterModuleOption(Option option) {
    final String type = "f";
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
  public class DatabaseModuleFactories {
    // left: import, right: export
    private final ImmutablePair<DatabaseModuleFactory, DatabaseModuleFactory> factories;
    private final List<DatabaseFilterFactory> filterFactories;
    /**
     * Create a new pair with an import module factory and an export module factory
     *
     * @param importModuleFactory
     *          the import module factory
     * @param exportModuleFactory
     *          the export module factory
     */
    public DatabaseModuleFactories(DatabaseModuleFactory importModuleFactory,
                                   DatabaseModuleFactory exportModuleFactory) {
      factories = new ImmutablePair<>(importModuleFactory, exportModuleFactory);
      filterFactories = new ArrayList<>();
    }

    public DatabaseModuleFactories(DatabaseModuleFactory importModuleFactory, DatabaseModuleFactory exportModuleFactory,
                                   List<DatabaseFilterFactory> filterFactories) {
      factories = new ImmutablePair<>(importModuleFactory, exportModuleFactory);
      this.filterFactories = filterFactories;
    }

    /**
     * @return the import module
     */
    public DatabaseModuleFactory getImportModuleFactory() {
      return factories.getLeft();
    }

    /**
     * @return the import module
     */
    public DatabaseModuleFactory getExportModuleFactory() {
      return factories.getRight();
    }

    public List<DatabaseFilterFactory> getFilterFactories() {
      return filterFactories;
    }
  }

  /**
   * Pair containing the arguments to create the import and export modules
   */
  public class DatabaseModuleFactoriesArguments {
    // left: import, right: export
    private final ImmutablePair<Map<Parameter, String>, Map<Parameter, String>> factories;
    private final List<Map<Parameter, String>> filterFactories;
    /**
     * Create a new pair with the import module arguments and the export module
     * arguments
     *
     * @param importModuleArguments
     *          import module arguments in the form Map<parameter, value parsed from
     *          the command line>
     * @param exportModuleArguments
     *          export module arguments in the form Map<parameter, value parsed from
     *          the command line>
     */
    public DatabaseModuleFactoriesArguments(Map<Parameter, String> importModuleArguments,
                                            Map<Parameter, String> exportModuleArguments) {
      factories = new ImmutablePair<>(importModuleArguments, exportModuleArguments);
      filterFactories = new ArrayList<>();
    }

    public DatabaseModuleFactoriesArguments(Map<Parameter, String> importModuleArguments,
                                            Map<Parameter, String> exportModuleArguments, List<Map<Parameter, String>> filterModulesArguments) {
      factories = new ImmutablePair<>(importModuleArguments, exportModuleArguments);
      filterFactories = filterModulesArguments;
    }

    /**
     * @return import module arguments in the form Map<parameter, value parsed from
     *         the command line>
     */
    public Map<Parameter, String> getImportModuleArguments() {
      return factories.getLeft();
    }

    /**
     * @return export module arguments in the form Map<parameter, value parsed from
     *         the command line>
     */
    public Map<Parameter, String> getExportModuleArguments() {
      return factories.getRight();
    }

    public List<Map<Parameter, String>> getFilterModulesArguments() {
      return filterFactories;
    }
  }
}
