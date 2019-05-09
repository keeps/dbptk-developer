/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.cli;

import com.databasepreservation.Constants;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class CLIHandler {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CLI.class);
  protected final List<String> commandLineArguments;

  public CLIHandler(List<String> commandLineArguments) {
    this.commandLineArguments = sanitizeCommandLineArguments(commandLineArguments);
  }

  /**
   * Replaces dashes (https://en.wikipedia.org/wiki/Dash#Common_dashes) with the
   * only supported "dash" character: '-'
   */
  protected List<String> sanitizeCommandLineArguments(List<String> commandLineArguments) {
    List<String> result = new ArrayList<>(commandLineArguments.size());
    Pattern pattern = Pattern.compile("^[\\u2012-\\u2015]([\\u2012-\\u2015])?");
    for (String commandLineArgument : commandLineArguments) {
      result.add(pattern.matcher(commandLineArgument).replaceAll("--"));
    }
    return result;
  }

  protected static String getUniqueOptionIdentifier(Option option) {
    // some string that should never occur in option shortName nor longName
    final String delimiter = "\r\f\n";
    return new StringBuilder().append(delimiter).append(option.getOpt()).append(delimiter).append(option.getLongOpt())
        .append(delimiter).toString();
  }

  /**
   * Removes the command argument from the {@link #commandLineArguments} variable
   *
   * @return true if succeed otherwise returns false
   */
  public boolean removeCommand() {

    String arg = commandLineArguments.get(0);

    if (arg.equalsIgnoreCase(Constants.DBPTK_OPTION_EDIT) ^ arg.equalsIgnoreCase(Constants.DBPTK_OPTION_MIGRATE)) {
      commandLineArguments.remove(0);
      return true;
    }
    return false;
  }
}
