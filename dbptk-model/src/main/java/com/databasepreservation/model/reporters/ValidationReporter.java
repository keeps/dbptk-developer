/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.reporters;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.utils.MiscUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ValidationReporter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationReporter.class);
  private Path outputFile;
  private BufferedWriter writer;
  private int numberOfPassed;
  private int numberOfErrors;
  private int numberOfWarnings;
  private int numberOfSkipped;
  private int numberOfRequirementsFailed;
  private int numberRequirementsPassed;

  public enum Indent {
    TAB, TAB_2, TAB_4
  }

  private static final String EMPTY_MESSAGE_LINE = "";
  private static final String NEWLINE = System.getProperty("line.separator", "\n");
  private static final String TAB = "\t";
  private static final String TAB_2 = "\t\t";
  private static final String TAB_4 = "\t\t\t";
  private static final String COLON = ":";
  private static final String SINGLE_SPACE = " ";
  private static final String OPEN_BRACKET = "[";
  private static final String OPEN_PARENTHESES = "(";
  private static final String CLOSED_BRACKET = "]";
  private static final String CLOSED_PARENTHESES = ")";
  private static final String HYPHEN = "-";

  public ValidationReporter(Path path, Path SIARDPackagePath) {
    init(path, SIARDPackagePath);
  }

  private void init(Path path, Path SIARDPackagePath) {
    this.outputFile = path;
    if (Files.notExists(outputFile)) {
      try {
        Files.createFile(outputFile);
      } catch (IOException e) {
        LOGGER.warn("Could not create report file in current working directory. Attempting to use a temporary file", e);
        try {
          outputFile = Files.createTempFile(Constants.DBPTK_VALIDATION_REPORTER_PREFIX, ".txt");
        } catch (IOException e1) {
          LOGGER.error("Could not create report temporary file. Reporting will not function.", e1);
        }
      }
    }

    if (outputFile != null) {
      try {
        writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8);
        writeLine(
          "######################################################################################################################################");
        writeLine(
          "#                                                       DBPTK - Validation Report                                                    #");
        writeLine(
          "######################################################################################################################################");
        writeLine("DBPTK Version: " + MiscUtils.APP_NAME_AND_VERSION);
        writeLine("SIARD Version: " + Constants.SIARD_VERSION_21);
        writeLine("The specification to the SIARD can be found at: " + Constants.LINK_TO_SPECIFICATION);
        writeLine("Additional checks specification can be found at: " + Constants.LINK_TO_WIKI_ADDITIONAL_CHECKS);
        writeLine("Date: " + new org.joda.time.DateTime());
        writeLine("SIARD file: " + SIARDPackagePath.toAbsolutePath().normalize().toString());
        writeLine(NEWLINE);
      } catch (IOException e) {
        LOGGER.error("Could not get a writer for the report file.", e);
      }
    }
  }

  public void moduleValidatorHeader(String text) {
    writeLine(text);
  }

  public void moduleValidatorHeader(String ID, String text) {
    writeLine(ID + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + text);
  }

  public void validationStatus(String text, ValidationReporterStatus status) {
    writeLine(TAB + text + COLON + SINGLE_SPACE + buildStatus(status));
  }

  public void validationStatus(String text, ValidationReporterStatus status, String details, Indent indent) {
    writeLine(resolveIndent(indent) + text + COLON + SINGLE_SPACE + buildStatus(status) + SINGLE_SPACE + details);
  }

  public void validationStatus(ValidationReporterStatus status, String details, Indent indent) {
    writeLine(resolveIndent(indent) + buildStatus(status) + SINGLE_SPACE + details);
  }

  public void validationStatus(String text, ValidationReporterStatus status, String details) {
    writeLine(TAB + text + COLON + SINGLE_SPACE + buildStatus(status) + SINGLE_SPACE + COLON + SINGLE_SPACE + details);
  }

  public void validationStatus(String text, ValidationReporterStatus status, String message, String path) {
    writeLine(TAB + text + COLON + SINGLE_SPACE + buildStatus(status) + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + message
      + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + path);
  }

  public void validationStatus(String text, ValidationReporterStatus status, String pathToEntry, List<String> details) {
    writeLine(TAB + text + COLON + SINGLE_SPACE + buildStatus(status));
    for (String detail : details) {
      writeLine(TAB + TAB + pathToEntry + COLON + SINGLE_SPACE + detail);
    }
  }

  public void validationStatus(String text, ValidationReporterStatus status, String pathToEntry,
    Map<String, List<String>> details) {
    writeLine(TAB + text + COLON + SINGLE_SPACE + buildStatus(status));
    for (Map.Entry<String, List<String>> entry : details.entrySet()) {
      for (String detail : entry.getValue()) {
        writeLine(TAB + TAB + pathToEntry + COLON + SINGLE_SPACE + detail);
      }
    }
  }

  public void moduleValidatorFinished(String text, ValidationReporterStatus status) {
    writeLine(text + SINGLE_SPACE + buildStatus(status));
    writeLine(EMPTY_MESSAGE_LINE);
  }

  public void warning(String ID, String text, String object) {
    writeLine(TAB + ID + COLON + SINGLE_SPACE + buildStatus(ValidationReporterStatus.WARNING) + SINGLE_SPACE + HYPHEN
      + SINGLE_SPACE + text + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + object);
  }

  public void skipValidation(String ID, String reasonToSkip) {
    writeLine(TAB + ID + COLON + SINGLE_SPACE + buildStatus(ValidationReporterStatus.SKIPPED) + SINGLE_SPACE + HYPHEN
      + SINGLE_SPACE + reasonToSkip);
  }

  public void notice(String ID, Object nodeValue, String noticeMessage) {
    writeLine(TAB + ID + COLON + SINGLE_SPACE + buildStatus(ValidationReporterStatus.NOTICE) + SINGLE_SPACE + HYPHEN
      + SINGLE_SPACE + noticeMessage + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + nodeValue);
  }

  public void notice(Object nodeValue, String noticeMessage) {
    writeLine(TAB + buildStatus(ValidationReporterStatus.NOTICE) + SINGLE_SPACE + HYPHEN + SINGLE_SPACE + noticeMessage
      + HYPHEN + SINGLE_SPACE + nodeValue);
  }

  public int getNumberOfWarnings() {
    return numberOfWarnings;
  }

  public int getNumberOfErrors() {
    return numberOfErrors;
  }

  public int getNumberOfPassed() {
    return numberOfPassed;
  }

  public int getNumberOfSkipped() {
    return numberOfSkipped;
  }

  public int getNumberOfRequirementsFailed() { return numberOfRequirementsFailed; }


  public int getNumberRequirementsPassed() {
    return numberRequirementsPassed;
  }

  private String buildStatus(ValidationReporterStatus status) {
    switch (status) {
      case FAILED:
        numberOfRequirementsFailed++;
        break;
      case PASSED:
        numberRequirementsPassed++;
        break;
      case ERROR:
        numberOfErrors++;
        break;
      case WARNING:
        numberOfWarnings++;
        break;
      case OK:
        numberOfPassed++;
        break;
      case SKIPPED:
        numberOfSkipped++;
        break;
    }
    return OPEN_BRACKET + status.name() + CLOSED_BRACKET;
  }

  private void writeLine(String line) {
    if (outputFile == null || writer == null) {
      LOGGER.info(line);
    } else {
      try {
        writer.write(line);
        writer.newLine();
      } catch (IOException e) {
        LOGGER.trace("IOException when trying to write report line", e);
        if (e.getMessage().equals("Stream closed")) {
          writer = null;
        }
        LOGGER.info(line);
      }
    }
  }

  private String resolveIndent(Indent indent) {
    switch (indent) {
      case TAB_2:
        return TAB_2;
      case TAB_4:
        return TAB_4;
      default:
        return TAB;
    }
  }

  @Override
  public void close() {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      LOGGER.debug("Unable to close validation reporter file", e);
    } finally {
      if (writer != null) {
        LOGGER.info("A report was generated with a listing of information about the individual validations.");
        LOGGER.info("The report file is located at {}", outputFile.normalize().toAbsolutePath());
      } else {
        LOGGER.info(
          "A report with a listing of information  about the individual validations could not be generated, please submit a bug report to help us fix this.");
      }
    }
  }
}