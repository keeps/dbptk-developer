/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.utils.ConfigUtils;
import com.databasepreservation.utils.MiscUtils;

/**
 * This reporter is used to report potential conversion problems/warnings.
 *
 * When using the Reporter in the Database Preservation Toolkit, the Main class
 * handles creating and providing the reporter to the import and export modules.
 *
 * When using the Reporter as a library import, a single instance should be
 * created per conversion job, and provided to the import and export modules
 * that will handling that conversion job. The Reporter should not be closed by
 * the modules, but by having the user code call the close() method.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Reporter implements AutoCloseable {
  // //////////////////////////////////////////////////
  // constants
  public static final String MESSAGE_FILTERED = "<filtered>";
  public static final String CODE_DELIMITER = "`";

  private static final Logger LOGGER = LoggerFactory.getLogger(Reporter.class);
  private static final String[] NUMBER_SUFFIXES = new String[] {"th", "st", "nd", "rd", "th", "th", "th", "th", "th",
    "th"};

  private static final String MESSAGE_LINE_PREFIX_ALL = "- ";
  private static final String MESSAGE_LINE_DEFAULT_PREFIX = "Information: ";
  private static final String MESSAGE_LINE_UPDATE_DEFAULT_PREFIX = "Updating: ";
  private static final String EMPTY_MESSAGE_LINE = "";
  private static final String NEWLINE = System.getProperty("line.separator", "\n");

  // //////////////////////////////////////////////////
  // instance variables
  private int countModuleInfoReported = 0;

  private long conversionProblemsCounter = 0;

  private boolean warnedAboutSavedAsString = false;

  private Path outputfile;
  private BufferedWriter writer;

  public Reporter() {
    this(null, null);
  }

  public Reporter(String directory, String name) {
    init(directory, name);
  }

  protected void init(String directory, String name) {
    // set defaults if needed
    if (StringUtils.isBlank(name)) {
      name = "dbptk-report-" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()) + ".txt";
    }

    if (StringUtils.isBlank(directory)) {
      outputfile = ConfigUtils.getReportsDirectory().resolve(name);
    } else {
      outputfile = Paths.get(directory).toAbsolutePath().resolve(name);
    }

    if (Files.notExists(outputfile)) {
      try {
        outputfile = Files.createFile(outputfile);
      } catch (IOException e) {
        LOGGER.warn("Could not create report file in current working directory. Attempting to use a temporary file", e);
        try {
          outputfile = Files.createTempFile("dbptk-report-", ".txt");
        } catch (IOException e1) {
          LOGGER.error("Could not create report temporary file. Reporting will not function.", e1);
        }
      }
    }

    if (outputfile != null) {
      try {
        writer = Files.newBufferedWriter(outputfile, StandardCharsets.UTF_8);

        String conversionReportLine = MiscUtils.APP_NAME_AND_VERSION + " -- Conversion Report";
        writeLine(conversionReportLine);
        writeLine(StringUtils.repeat('=', conversionReportLine.length()));
        writeLine(EMPTY_MESSAGE_LINE);
      } catch (IOException e) {
        LOGGER.error("Could not get a writer for the report file.", e);
      }
    }
  }

  private void writeLine(String line) {
    if (outputfile == null || writer == null) {
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

  private void report(StringBuilder message) {
    report(message, MESSAGE_LINE_PREFIX_ALL);
  }

  private void report(StringBuilder message, String prefix) {
    if (StringUtils.isNotBlank(prefix)) {
      message.insert(0, prefix);
    }
    writeLine(message.toString());
  }

  private void moduleParameters(String moduleName, String importOrExport, String... parameters) {
    StringBuilder message;

    if (countModuleInfoReported == 0) {
      message = new StringBuilder("## Parameters").append(NEWLINE).append(NEWLINE);
    } else {
      message = new StringBuilder();
    }

    message.append("**").append(importOrExport).append(" module:** ").append(moduleName);

    for (int i = 0; i < parameters.length; i += 2) {
      message.append(NEWLINE).append("- ").append(parameters[i]).append(" = ").append(parameters[i + 1]);
    }

    countModuleInfoReported++;

    LOGGER.debug("moduleParameters, module: " + moduleName + " with parameters " + message);
    if (countModuleInfoReported == 2) {
      message.append(NEWLINE).append(NEWLINE).append("Date: ")
        .append(new SimpleDateFormat("yyyy-MM-dd").format(new Date())).append(NEWLINE).append(NEWLINE)
        .append("## Details").append(NEWLINE);
    } else {
      message.append(NEWLINE);
    }
    report(message, null);
  }

  // //////////////////////////////////////////////////
  // public
  public void cellProcessingUsedNull(String tableId, String columnName, long rowIndex, Throwable exception) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder("Problem processing cell value and NULL was used instead, ");

    if (StringUtils.isNotBlank(tableId) && StringUtils.isNotBlank(columnName)) {
      message.append("in table ");
      appendAsCode(message, tableId).append(", in column ");
      appendAsCode(message, columnName).append(", ");
    } else if (StringUtils.isNotBlank(columnName)) {
      message.append("in column ");
      appendAsCode(message, columnName).append(", ");
    } else {
      message.append("in an unidentified table and column, ");
    }
    message.append("in the ").append(rowIndex).append(ordinal(rowIndex)).append(" row");

    report(message);
    LOGGER.debug("cellProcessingUsedNull, message: " + message, exception);
  }

  public void cellProcessingUsedNull(TableStructure table, ColumnStructure column, long rowIndex, Throwable exception) {
    cellProcessingUsedNull(table.getId(), column.getName(), rowIndex, exception);
  }

  public void failedToGetDescription(Exception e, String designation, String... scopes) {
    StringBuilder message = new StringBuilder(MESSAGE_LINE_DEFAULT_PREFIX);
    message.append("could not get description for ").append(designation);

    if (scopes.length > 0) {
      message.append(" (");
      appendAsCode(message, StringUtils.join(scopes, "."));
      message.append(")");
    }

    report(message);
    LOGGER.debug("failedToGetDescription, message: {}", message.toString(), e);
  }

  public void rowProcessingUsedNull(TableStructure table, long rowIndex, Throwable exception) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder(
      "Problem processing row values and NULL was used instead for all cells, in table ");
    appendAsCode(message, table.getId()).append(" in the ").append(rowIndex).append(ordinal(rowIndex)).append(" row");

    report(message);
    LOGGER.debug("cellProcessingUsedNull, message: " + message, exception);
  }

  public void notYetSupported(String feature, String module) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder(MESSAGE_LINE_DEFAULT_PREFIX).append(feature)
      .append(" is not yet supported for ").append(module).append(". But support may be added in the future");

    report(message);
    LOGGER.debug("notYetSupported, message: " + message);
  }

  public void dataTypeChangedOnImport(String invokerNameForDebug, String schemaName, String tableName,
    String columnName, Type type) {
    String original = null;
    if (type != null && StringUtils.isNotBlank(type.getOriginalTypeName())) {
      original = type.getOriginalTypeName().toUpperCase(Locale.ENGLISH);
    }

    String sql99 = type.getSql99TypeName();
    if (StringUtils.isNotBlank(sql99)) {
      sql99 = sql99.toUpperCase(Locale.ENGLISH);
    } else {
      sql99 = "(unknown)";
    }
    String sql2008 = type.getSql2008TypeName();
    if (StringUtils.isNotBlank(sql2008)) {
      sql2008 = sql2008.toUpperCase(Locale.ENGLISH);
    } else {
      sql2008 = "(unknown)";
    }

    StringBuilder message = new StringBuilder("Type conversion in import module: in ");
    appendAsCode(message, schemaName + "." + tableName + "." + columnName);
    message.append(" (format: schema.table.column) has ");

    if (original == null) {
      message.append("an unidentified original type");
    } else {
      message.append("original type ");
      appendAsCode(message, original);
    }

    message.append(" and was converted to the standard type ");

    if (sql99.equals(sql2008)) {
      appendAsCode(message, sql99);
    } else {
      appendAsCode(message, sql99).append(" and ");
      appendAsCode(message, sql2008).append(" (SQL99 and SQL2008 standard)");
    }

    // report only if the data type actually changed
    if (original != null && (!original.equals(sql99) || !original.equals(sql2008))) {
      conversionProblemsCounter++;
      report(message);
      LOGGER.debug(
        "dataTypeChangedOnImport, invoker: " + invokerNameForDebug + "; message: " + message + "; and type: " + type);
    }
  }

  public void dataTypeChangedOnExport(String invokerNameForDebug, ColumnStructure column, String typeSQL) {
    String original = column.getType().getOriginalTypeName().toUpperCase(Locale.ENGLISH);
    String sql99 = column.getType().getSql99TypeName().toUpperCase(Locale.ENGLISH);
    String sql2008 = column.getType().getSql2008TypeName().toUpperCase(Locale.ENGLISH);

    StringBuilder message = new StringBuilder("Type conversion in export module: in ");
    appendAsCode(message, column.getId()).append(" (format: schema.table.column) has original type ");
    appendAsCode(message, original).append(" and standard type ");

    if (sql99.equals(sql2008)) {
      appendAsCode(message, sql99);
    } else {
      appendAsCode(message, sql99).append(" and ");
      appendAsCode(message, sql2008).append(" (SQL99 and SQL2008 standard)");
    }

    message.append(", will be created as ");
    appendAsCode(message, typeSQL).append(" in the target database");

    // report only if the data type actually changed
    if (!original.equals(sql99) || !original.equals(sql2008)) {
      conversionProblemsCounter++;
      report(message);
      LOGGER.debug("dataTypeChangedOnExport, invoker: " + invokerNameForDebug + "; message: " + message
        + "; and column: " + column);
    }
  }

  public void exportModuleParameters(String moduleName, String... parameters) {
    moduleParameters(moduleName, "Export", parameters);
  }

  public void importModuleParameters(String moduleName, String... parameters) {
    moduleParameters(moduleName, "Import", parameters);
  }

  public void customMessage(String invokerNameForDebug, String customMessage, String prefix) {
    StringBuilder message = new StringBuilder();
    if (prefix != null) {
      message.append(prefix).append(": ");
    } else {
      message.append(MESSAGE_LINE_DEFAULT_PREFIX);
    }
    message.append(customMessage);
    report(message);
    LOGGER.debug("customMessage, invoker: " + invokerNameForDebug + "; message: " + message.toString());
  }

  public void customMessage(String invokerNameForDebug, String customMessage) {
    customMessage(invokerNameForDebug, customMessage, null);
  }

  public void savedAsString() {
    if (!warnedAboutSavedAsString) {
      warnedAboutSavedAsString = true;
      report(new StringBuilder("Found an unsupported datatype value, and an attempt was made to save it as text."));
    }
  }

  public void ignored(String whatWasIgnored, String whyItWasIgnored) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder(MESSAGE_LINE_DEFAULT_PREFIX);
    appendAsCode(message, whatWasIgnored).append(" was ignored because ").append(whyItWasIgnored);

    report(message);
    LOGGER.debug("something was ignored, message: " + message);
  }

  public void failed(String whatFailed, String whyItFailed) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder(MESSAGE_LINE_DEFAULT_PREFIX).append(whatFailed).append(" failed because ")
      .append(whyItFailed);

    report(message);
    LOGGER.debug("something failed, message: " + message);
  }

  public void valueChanged(String originalValue, String newValue, String reason, String location) {
    conversionProblemsCounter++;
    StringBuilder message = new StringBuilder("Warning: ");
    appendAsCode(message, originalValue).append(" changed to ");
    appendAsCode(message, newValue).append(" because ").append(reason).append(" in ").append(location);

    report(message);
    LOGGER.debug("something failed, message: " + message);
  }

  public void metadataUpdated(String message) {
    metadataUpdated(message, null);
  }

  public void metadataUpdated(String message, String prefix) {
    StringBuilder sb;
    if (prefix != null) {
      sb = new StringBuilder(prefix);
    } else {
      sb = new StringBuilder(MESSAGE_LINE_UPDATE_DEFAULT_PREFIX);
    }

    sb.append(message);

    report(sb);

    LOGGER.info(message);
  }

  public void metadataParameters(String moduleName, List<SIARDDatabaseMetadata> parameters) {
    StringBuilder message;

    message = new StringBuilder("## Set").append(NEWLINE);

    for (SIARDDatabaseMetadata metadata : parameters) {
      message.append(NEWLINE).append("- ").append(metadata.toString()).append(", with value: '")
        .append(metadata.getValue()).append("'");
    }

    message.append(NEWLINE).append(NEWLINE);

    LOGGER.debug("moduleParameters, module: " + moduleName + " with parameters " + message);

    report(message, null);
  }

  /**
   * Closes the Reporter instance and underlying resources. And also logs the
   * location of the Reporter file.
   *
   * @throws IOException
   *           if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    try {
      if (writer != null) {
        if (conversionProblemsCounter == 0) {
          report(new StringBuilder("No issues to report."));
        }
        writer.close();
      }
    } catch (IOException e) {
      LOGGER.debug("Unable to close Reporter file", e);
    } finally {
      if (conversionProblemsCounter != 0) {
        if (writer != null) {
          LOGGER.info("A report was generated with a listing of information that was modified during the conversion.");
          LOGGER.info("The report file is located at {}", outputfile.normalize().toAbsolutePath().toString());
        } else {
          LOGGER.info(
            "A report with a listing of information that was modified during the conversion could not be generated, please submit a bug report to help us fix this.");
        }
      }
    }
  }

  /**
   * Adds the code string delimited by the CODE_DELIMITER to the string builder
   */
  private static StringBuilder appendAsCode(StringBuilder sb, String code) {
    sb.append(CODE_DELIMITER).append(code).append(CODE_DELIMITER);
    return sb;
  }

  /**
   * Utility to get the ordinal suffix for a number
   */
  private static String ordinal(long i) {
    switch ((int) i % 100) {
      case 11:
      case 12:
      case 13:
        return NUMBER_SUFFIXES[0];
      default:
        return NUMBER_SUFFIXES[(int) i % 10];

    }
  }
}
