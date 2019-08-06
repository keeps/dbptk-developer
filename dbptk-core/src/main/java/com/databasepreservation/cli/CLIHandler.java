/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import com.databasepreservation.Constants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class CLIHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(CLI.class);
  final List<String> commandLineArguments;

  CLIHandler(List<String> commandLineArguments) {
    this.commandLineArguments = sanitizeCommandLineArguments(commandLineArguments);
  }

  /**
   * Replaces dashes (https://en.wikipedia.org/wiki/Dash#Common_dashes) with the
   * only supported "dash" character: '-'
   */
  private List<String> sanitizeCommandLineArguments(List<String> commandLineArguments) {
    List<String> result = new ArrayList<>(commandLineArguments.size());
    Pattern pattern = Pattern.compile("^[\\u2012-\\u2015]([\\u2012-\\u2015])?");
    for (String commandLineArgument : commandLineArguments) {
      result.add(pattern.matcher(commandLineArgument).replaceAll("--"));
    }
    return result;
  }

  static String getUniqueOptionIdentifier(Option option) {
    // some string that should never occur in option shortName nor longName
    final String delimiter = "\r\f\n";
    return delimiter + option.getOpt() + delimiter + option.getLongOpt() +
        delimiter;
  }

  /**
   * Removes the command argument from the {@link #commandLineArguments} variable
   *
   * @return true if succeed otherwise returns false
   */
  public boolean removeCommand() {

    String arg = commandLineArguments.get(0);

    if (arg.equalsIgnoreCase(Constants.DBPTK_OPTION_EDIT)
        ^ arg.equalsIgnoreCase(Constants.DBPTK_OPTION_MIGRATE)
        ^ arg.equalsIgnoreCase(Constants.DBPTK_OPTION_VALIDATE)) {
      commandLineArguments.remove(0);
      return true;
    }
    return false;
  }

  /**
   * Checks if the command line arguments are empty
   *
   * @return true if empty otherwise false.
   */
  public boolean emptyArguments() {
    return commandLineArguments.isEmpty();
  }

  CommandLine commandLineParse(CommandLineParser commandLineParser, Options options, List<String> args) throws ParseException {
    CommandLine commandLine;
    // parse the command line arguments with those options
    try {
      commandLine = commandLineParser.parse(options, args.toArray(new String[]{}), false);
      if (!commandLine.getArgList().isEmpty()) {
        throw new ParseException("Unrecognized option: " + commandLine.getArgList().get(0));
      }
    } catch (MissingOptionException e) {
      // use long names instead of short names in the error message
      List missingShort = e.getMissingOptions();
      List<String> missingLong = new ArrayList<>();
      for (Object shortOption : missingShort) {
        missingLong.add(options.getOption(shortOption.toString()).getLongOpt());
      }
      LOGGER.debug("MissingOptionException (the original, unmodified exception)", e);
      throw new MissingOptionException(missingLong);
    } catch (MissingArgumentException e) {
      // use long names instead of short names in the error message
      Option faulty = e.getOption();
      LOGGER.debug("MissingArgumentException (the original, unmodified exception)", e);
      throw new MissingArgumentException("Missing the argument for the " + e.getOption().getLongOpt() + " option");
    }

    return commandLine;
  }
}
