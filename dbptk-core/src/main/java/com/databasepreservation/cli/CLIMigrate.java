/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CLIMigrate extends CLIHandler {
  private static final String IMPORT_PREFIX = "import";
  private static final String EXPORT_PREFIX = "export";
  private static final String FILTER_PREFIX = "filter";

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
    allModuleFactories = new ArrayList<>(databaseModuleFactories);
    allFilterFactories = new ArrayList<>();
    filterNames = new ArrayList<>();
  }

  public CLIMigrate(List<String> commandLineArguments, Collection<DatabaseModuleFactory> databaseModuleFactories,
    Collection<DatabaseFilterFactory> databaseFilterFactories) {
    super(commandLineArguments);
    allModuleFactories = new ArrayList<>(databaseModuleFactories);
    allFilterFactories = new ArrayList<>(databaseFilterFactories);
    filterNames = new ArrayList<>();
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
  public String getImportModuleName() {
    return importModuleName;
  }

  public List<String> getFilterNames() {
    return filterNames;
  }

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

      DatabaseModuleFactoriesArguments databaseModuleFactoriesArguments = getModuleArguments(databaseModuleFactories,
        args);

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
    String pImportModuleName = null;
    String pExportModuleName = null;
    List<String> filterModuleNames = new ArrayList<>();

    int importModulesFound = 0;
    int exportModulesFound = 0;

    Iterator<String> argsIterator = args.iterator();
    try {
      while (argsIterator.hasNext()) {
        String arg = argsIterator.next();
        if ("-i".equals(arg) || "--import".equals(arg)) {
          pImportModuleName = argsIterator.next();
          importModulesFound++;
        } else if ("-e".equals(arg) || "--export".equals(arg)) {
          pExportModuleName = argsIterator.next();
          exportModulesFound++;
        } else if (StringUtils.startsWith(arg, "--import=")) {
          // 9 is the size of the string "--import="
          pImportModuleName = arg.substring(9);
          importModulesFound++;
        } else if (StringUtils.startsWith(arg, "--export=")) {
          // 9 is the size of the string "--export="
          pExportModuleName = arg.substring(9);
          exportModulesFound++;
        } else if ("-f".equals(arg) || "--filter".equals(arg) || "--filters".equals(arg)) {
          String pFilterNames = argsIterator.next();
          filterModuleNames = Arrays.asList(pFilterNames.split(","));
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
    DatabaseModuleFactory pImportModuleFactory = null;
    DatabaseModuleFactory pExportModuleFactory = null;
    List<DatabaseFilterFactory> filterModuleFactories = new ArrayList<>();
    for (DatabaseModuleFactory factory : allModuleFactories) {
      String moduleName = factory.getModuleName();
      if (moduleName.equalsIgnoreCase(pImportModuleName) && factory.producesImportModules()) {
        pImportModuleFactory = factory;
      }
      if (moduleName.equalsIgnoreCase(pExportModuleName) && factory.producesExportModules()) {
        pExportModuleFactory = factory;
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

    if (pImportModuleFactory == null) {
      throw new ParseException("Invalid import module.");
    } else if (pExportModuleFactory == null) {
      throw new ParseException("Invalid export module.");
    }
    return new DatabaseModuleFactories(pImportModuleFactory, pExportModuleFactory, filterModuleFactories);
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
    DatabaseModuleFactory pImportModuleFactory = factories.getImportModuleFactory();
    DatabaseModuleFactory pExportModuleFactory = factories.getExportModuleFactory();
    List<DatabaseFilterFactory> pFilterFactories = factories.getFilterFactories();

    // get appropriate command line options
    CommandLineParser commandLineParser = new DefaultParser();
    CommandLine commandLine;
    Options options = new Options();

    Map<String, Parameter> mapOptionToParameter = new HashMap<>();

    for (Parameter parameter : pImportModuleFactory.getImportModuleParameters().getParameters()) {
      Option option = parameter.toOption("i", IMPORT_PREFIX);
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }

    for (ParameterGroup parameterGroup : pImportModuleFactory.getImportModuleParameters().getGroups()) {
      OptionGroup optionGroup = parameterGroup.toOptionGroup("i", IMPORT_PREFIX);
      options.addOptionGroup(optionGroup);

      for (Parameter parameter : parameterGroup.getParameters()) {
        mapOptionToParameter.put(getUniqueOptionIdentifier(parameter.toOption("i", IMPORT_PREFIX)), parameter);
      }
    }

    for (Parameter parameter : pExportModuleFactory.getExportModuleParameters().getParameters()) {
      Option option = parameter.toOption("e", EXPORT_PREFIX);
      options.addOption(option);
      mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
    }

    for (ParameterGroup parameterGroup : pExportModuleFactory.getExportModuleParameters().getGroups()) {
      OptionGroup optionGroup = parameterGroup.toOptionGroup("e", EXPORT_PREFIX);
      options.addOptionGroup(optionGroup);

      for (Parameter parameter : parameterGroup.getParameters()) {
        mapOptionToParameter.put(getUniqueOptionIdentifier(parameter.toOption("e", EXPORT_PREFIX)), parameter);
      }
    }

    for (int i = 0; i < pFilterFactories.size(); i++) {
      for (Parameter parameter : pFilterFactories.get(i).getParameters().getParameters()) {
        Option option = parameter.toOption("f" + (i + 1), FILTER_PREFIX + (i + 1));
        options.addOption(option);
        mapOptionToParameter.put(getUniqueOptionIdentifier(option), parameter);
      }
    }

    Option importOption = Option.builder("i").longOpt(IMPORT_PREFIX).hasArg().optionalArg(false).build();
    Option exportOption = Option.builder("e").longOpt(EXPORT_PREFIX).hasArg().optionalArg(false).build();
    Option filtersOption = Option.builder("f").longOpt(FILTER_PREFIX).hasArg().optionalArg(false).required(false)
      .build();
    options.addOption(importOption);
    options.addOption(exportOption);
    options.addOption(filtersOption);

    commandLine = commandLineParse(commandLineParser, options, args);

    // create arguments to pass to factory
    Map<Parameter, String> importModuleArguments = new HashMap<>();
    Map<Parameter, String> exportModuleArguments = new HashMap<>();
    List<Map<Parameter, String>> filterModuleArguments = new ArrayList<>();
    for (int i = 0; i < pFilterFactories.size(); i++) {
      filterModuleArguments.add(new HashMap<>());
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
          try (Scanner scanner = new Scanner(option.getOpt())) {
            int filterIndex = scanner.useDelimiter("\\D+").nextInt();

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
