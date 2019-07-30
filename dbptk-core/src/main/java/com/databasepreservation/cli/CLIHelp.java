/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.UnreachableException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.ParameterGroup;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.utils.MiscUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CLIHelp extends CLIHandler {

  private ArrayList<DatabaseModuleFactory> allModuleFactories;
  private ArrayList<DatabaseFilterFactory> allFilterFactories;
  private ArrayList<EditModuleFactory> allEditModuleFactories;

  public CLIHelp(List<String> commandLineArguments) {
    super(commandLineArguments);
  }

  public void setDatabaseModuleFactory(Collection<DatabaseModuleFactory> databaseModuleFactory) {
    this.allModuleFactories = new ArrayList<>(databaseModuleFactory);
  }

  public void setDatabaseFilterFactory(Collection<DatabaseFilterFactory> databaseFilterFactory) {
    this.allFilterFactories = new ArrayList<>(databaseFilterFactory);
  }

  public void setEditModuleFactories(Collection<EditModuleFactory> editModuleFactories) {
    this.allEditModuleFactories = new ArrayList<>(editModuleFactories);
  }

  public void printHelp(PrintStream printStream) {
    printInternalHelp(printStream);
  }

  public void printUsage(PrintStream printStream) {
    printInternalUsage(printStream);
  }

  public void printEditUsage(PrintStream printStream) { printInternalEditUsage(printStream); }

  /**
   * Prints the help text
   *
   * @param printStream
   */
  private void printInternalHelp(PrintStream printStream) {
    if (commandLineArguments.size() <= 1) {
      printInternalUsage(printStream);
    } else {
      String arg = commandLineArguments.get(1);

      switch (arg) {
        case Constants.DBPTK_OPTION_EDIT: printInternalEditUsage(printStream);
          break;
        case Constants.DBPTK_OPTION_MIGRATE: printMigrateUsage(printStream);
          break;
        default: printUsage(printStream);
      }
    }
  }

  /**
   * Prints the header
   *
   * @param printStream
   */
  private void printHeader(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    out.append("Database Preservation Toolkit").append(MiscUtils.APP_NAME_AND_VERSION)
        .append("\n")
        .append("More info: http://www.database-preservation.com")
        .append("\n");

    out.append("\n");
    printStream.append(out).flush();
  }

  /**
   * Prints the usage text
   *
   * @param printStream
   */
  private void printInternalUsage(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    printHeader(printStream);

    out.append("Usage: dbptk COMMAND [OPTIONS]\n");

    out.append("\n");
    out.append("Commands:");
    out.append("\n\n");
    out.append("\t").append(Constants.DBPTK_OPTION_MIGRATE).append("\t\t").append("Migrates data and metadata from an import module to an export module.").append("\n");
    out.append("\t").append(Constants.DBPTK_OPTION_EDIT).append("\t\t").append("Edit the metadata information from a SIARD archive.").append("\n");

    out.append("\n");
    out.append("Run 'dbptk -h|help COMMAND' for more information on a command.").append("\n");

    out.append("\n");
    printStream.append(out).flush();
  }

  /**
   * Prints the edit command usage text
   *
   * @param printStream
   */
  protected void printInternalEditUsage(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    printHeader(printStream);

    out.append("Usage: dbptk edit [OPTIONS]");
    out.append("\n\n");

    out.append("Options: ");

    EditModuleFactory editModuleFactory = allEditModuleFactories.get(0);

    for (Parameter parameter : editModuleFactory.getImportParameters().getParameters()) {
      out.append(printParameterHelp(Constants.SMALL_SPACE, "i", "import", parameter));
    }

    for (Parameter parameter : editModuleFactory.getParameters().getParameters()) {
      out.append(printParameterHelp(Constants.SMALL_SPACE, parameter));
    }

    out.append("\n\n");

    out.append("Note: In case you want to export all metadata pairs for the SIARD2 archive, redirect the output to a file using the '>' symbol\n");

    out.append("\n\n");
    printStream.append(out).flush();
  }

  /**
   * Prints the migrate usage text
   *
   * @param printStream
   */
  private void printMigrateUsage(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    printHeader(printStream);

    out.append("Usage: dbptk migrate <importModule> [import module options] <exportModule> [export module options] [<filterModule(s)> [filter module options]]");
    out.append("\n");

    out.append("\n");
    printStream.append(out).flush();

    if (commandLineArguments.size() >= 3) {
      printMigrateModuleHelp(printStream);
    } else {
      printMigrateHelp(printStream);
    }
  }

  /**
   * Prints the migrate module text
   *
   * @param printStream
   */
  private void printMigrateModuleHelp(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    int count=0;

    Set<String> visibleModules = new HashSet<>(commandLineArguments);

    ArrayList<DatabaseModuleFactory> modulesList = new ArrayList<>(allModuleFactories);
    Collections.sort(modulesList, new CLIMigrate.DatabaseModuleFactoryNameComparator());

    ArrayList<DatabaseFilterFactory> filterModulesList = new ArrayList<>(allFilterFactories);
    Collections.sort(modulesList, new CLIMigrate.DatabaseModuleFactoryNameComparator());

    try {
      for (DatabaseModuleFactory factory : modulesList) {
        if (factory.producesImportModules() && visibleModules.contains(factory.getModuleName())) {
          count++;
          out.append(
              printModuleHelp("Import module: -i " + factory.getModuleName() + ", --import=" + factory.getModuleName(),
                  "i", "import", factory.getImportModuleParameters()));
        }
        if (factory.producesExportModules() && visibleModules.contains(factory.getModuleName())) {
          count++;
          out.append(
              printModuleHelp("Export module: -e " + factory.getModuleName() + ", --export=" + factory.getModuleName(),
                  "e", "export", factory.getExportModuleParameters()));
        }
      }

      for (DatabaseFilterFactory factory : filterModulesList) {
        if (visibleModules.contains(factory.getFilterName()) && factory.isEnabled()) {
          count++;
          out.append(printModuleHelp(
              "Filter module: -f " + factory.getFilterName() + ", --filter=" + factory.getFilterName(), "f<n>",
              "filter<n>", factory.getParameters()));
        }
      }

      if (count == 0) {
        printMigrateHelp(printStream);
      }

      out.append("\n");
      printStream.append(out).flush();
    } catch (UnsupportedModuleException e) {
      throw new UnreachableException(e);
    }
  }


  /**
   * Prints the migrate text
   *
   * @param printStream
   */
  private void printMigrateHelp(PrintStream printStream) {
    StringBuilder out = new StringBuilder();

    ArrayList<DatabaseModuleFactory> modulesList = new ArrayList<>(allModuleFactories);
    Collections.sort(modulesList, new CLIMigrate.DatabaseModuleFactoryNameComparator());

    ArrayList<DatabaseFilterFactory> filterModulesList = new ArrayList<>(allFilterFactories);
    Collections.sort(filterModulesList, new CLIMigrate.DatabaseFilterFactoryNameComparator());

    out.append("\n").append(Constants.SMALL_SPACE).append("Import modules: \n");

    for (DatabaseModuleFactory factory : modulesList) {
      if (factory.producesImportModules()) {
        out.append(Constants.MEDIUM_SPACE).append(factory.getModuleName()).append("\n");
      }
    }

    out.append("\n\n").append(Constants.SMALL_SPACE).append("Export modules: \n");
    for (DatabaseModuleFactory factory : modulesList) {
      if (factory.producesExportModules()) {
        out.append(Constants.MEDIUM_SPACE).append(factory.getModuleName()).append("\n");
      }
    }

    out.append("\n\n").append(Constants.SMALL_SPACE).append("Filter modules: \n");
    for (DatabaseFilterFactory factory : filterModulesList) {
      if (factory.isEnabled()) {
        out.append(Constants.MEDIUM_SPACE).append(factory.getFilterName()).append("\n");
      }
    }

    out.append("\n");
    out.append("Run 'dbptk -h|help migrate [module ...]' for more information on a command.").append("\n");

    out.append("\n");
    printStream.append(out).flush();
  }

  private String printModuleHelp(String moduleDesignation, String shortParameterPrefix, String longParameterPrefix,
                                 Parameters moduleParameters) {
    StringBuilder out = new StringBuilder();

    out.append("\n").append(Constants.SMALL_SPACE).append(moduleDesignation);

    for (Parameter parameter : moduleParameters.getParameters()) {
      out.append(printParameterHelp(Constants.SMALL_SPACE, shortParameterPrefix, longParameterPrefix, parameter));
    }

    for (ParameterGroup parameterGroup : moduleParameters.getGroups()) {
      for (Parameter parameter : parameterGroup.getParameters()) {
        out.append(printParameterHelp(Constants.SMALL_SPACE, shortParameterPrefix, longParameterPrefix, parameter));
      }
    }
    out.append("\n");

    return out.toString();
  }

  private String printParameterHelp(String space, Parameter parameter) {
    StringBuilder out = new StringBuilder();

    out.append("\n").append(space).append(space);

    if (StringUtils.isNotBlank(parameter.shortName())) {
      out.append("-").append(parameter.shortName()).append(", ");
    }

    out.append("--").append(parameter.longName());

    if (parameter.hasArgument()) {
      if (parameter.numberOfArgs() != null && parameter.numberOfArgs() > 1) {
        out.append(" key value");
      } else {
        if (parameter.isOptionalArgument()) {
          out.append("[");
        }
        out.append("=value");
        if (parameter.isOptionalArgument()) {
          out.append("]");
        }
      }
    }

    out.append("\n").append(space).append(space).append(space);
    String description = (parameter.required() ? "(required) " : "(optional) ") + parameter.description();
    out.append(WordUtils.wrap(description, Constants.CLI_LINE_WIDTH, "\n" + StringUtils.repeat(space, 3), false));

    return out.toString();
  }

  private String printParameterHelp(String space, String shortPrefix, String longPrefix, Parameter parameter) {
    StringBuilder out = new StringBuilder();

    out.append("\n").append(space).append(space);

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

    out.append("\n").append(space).append(space).append(space);
    String description = (parameter.required() ? "(required) " : "(optional) ") + parameter.description();
    out.append(WordUtils.wrap(description, Constants.CLI_LINE_WIDTH, "\n" + StringUtils.repeat(space, 3), false));

    return out.toString();
  }
}
