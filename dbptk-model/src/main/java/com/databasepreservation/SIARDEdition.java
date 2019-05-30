/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.EditDatabaseMetadataParserException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.metadata.SIARDDatabaseMetadata;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.utils.JodaUtils;
import com.databasepreservation.utils.PrintUtils;

/**
 * Class responsible for handling all the logic for update database level
 * metadata from a SIARD version 2 archive.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEdition {
  private EditModuleFactory editModuleFactory;
  private HashMap<Parameter, List<String>> editModuleParameters;
  private static HashMap<SIARDDatabaseMetadata, String> cliInputParameters = new HashMap<>();
  private Reporter reporter;

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDEdition.class);

  public static SIARDEdition newInstance() {
    return new SIARDEdition();
  }

  /**
   * Sets the edit module factory that will be used to produce the edit module
   */
  public SIARDEdition editModule(EditModuleFactory factory) {
    this.editModuleFactory = factory;
    return this;
  }

  /**
   * Adds the specified parameter to be used in the edit module during the edition
   */
  public SIARDEdition editModuleParameters(Map<Parameter, List<String>> parameters) {
    this.editModuleParameters = new HashMap<>(parameters);
    return this;
  }

  /**
   * Sets the reporter to be used by all modules during the edition
   */
  public SIARDEdition reporter(Reporter reporter) {
    this.reporter = reporter;
    return this;
  }

  /**
   * lists all the possible changes in a SIARD archive.
   *
   * @throws ModuleException
   *           Generic module exception
   */
  public void list() throws ModuleException {
    HashMap<Parameter, String> importParameters = buildImportParameters(editModuleParameters, editModuleFactory);

    EditModule editModule = editModuleFactory.buildModule(importParameters, reporter);
    editModule.setOnceReporter(reporter);

    DatabaseStructure metadata = editModule.getMetadata();

    PrintUtils.printDatabaseStructureTree(metadata, System.out);
  }

  /**
   * Edits the <code>DatabaseStructure</code>, updating a certain attribute to a
   * new value.
   *
   * @throws EditDatabaseMetadataParserException
   *           The metadata path is not a valid one
   * @throws ModuleException
   *           Generic module exception
   */
  public void edit() throws ModuleException {

    HashMap<Parameter, String> importParameters = buildImportParameters(editModuleParameters, editModuleFactory);
    List<SIARDDatabaseMetadata> metadataPairs = buildMetadataPairs(editModuleParameters, editModuleFactory);

    EditModule editModule = editModuleFactory.buildModule(importParameters, reporter);
    reporter.metadataParameters(editModuleFactory.getModuleName(), metadataPairs);
    editModule.setOnceReporter(reporter);

    // COMES FROM THE XSD FILE
    List<SIARDDatabaseMetadata> descriptiveMetadataKeys = editModule.getDescriptiveSIARDMetadataKeys();
    // COMES FROM THE XML FILE
    List<SIARDDatabaseMetadata> SIARDDatabaseMetadataKeys = editModule.getDatabaseMetadataKeys();

    List<String> malformedMetadataKeys = validateMetadataKeys(descriptiveMetadataKeys, SIARDDatabaseMetadataKeys,
      metadataPairs);

    if (malformedMetadataKeys.isEmpty()) {
      DatabaseStructure metadata = editModule.getMetadata();

      DatabaseStructure updated = update(metadata, metadataPairs);
      editModule.updateMetadata(updated);
    } else {
      if (malformedMetadataKeys.size() == 1) {
        throw new EditDatabaseMetadataParserException("Metadata path not found")
          .withFaultyArgument(malformedMetadataKeys.get(0));
      } else {
        throw new EditDatabaseMetadataParserException("Invalid metadata paths")
          .withFaultyArgument(malformedMetadataKeys.toString());
      }
    }
  }

  // Auxiliary Internal Methods

  private static HashMap<Parameter, String> buildImportParameters(HashMap<Parameter, List<String>> editModuleParameters,
    EditModuleFactory editModuleFactory) {

    HashMap<Parameter, String> importParameters = new HashMap<>();

    for (Map.Entry<Parameter, List<String>> entry : editModuleParameters.entrySet()) {
      for (Parameter p : editModuleFactory.getAllParameters().keySet()) {
        if (entry.getKey().equals(p)) {
          if (p.longName().contentEquals("file")) {
            importParameters.put(p, entry.getValue().get(0));
          }
        }
      }
    }

    return importParameters;
  }

  private static List<SIARDDatabaseMetadata> buildMetadataPairs(HashMap<Parameter, List<String>> editModuleParameters,
    EditModuleFactory editModuleFactory) throws ModuleException {

    List<SIARDDatabaseMetadata> metadata = new ArrayList<>();

    for (Map.Entry<Parameter, List<String>> entry : editModuleParameters.entrySet()) {
      for (String s : entry.getValue()) {
        for (Parameter p : editModuleFactory.getSetParameters().keySet()) {
          if (entry.getKey().equals(p)) {
            SIARDDatabaseMetadata parsedKey = parse(s);
            metadata.add(parsedKey);
            String normalized = StringUtils.replace(s, Constants.SEPARATOR, " ");
            cliInputParameters.put(parsedKey, normalized);
          }
        }
      }
    }

    return metadata;
  }

  private static List<String> validateMetadataKeys(List<SIARDDatabaseMetadata> validDescriptiveMetadataKeys,
    List<SIARDDatabaseMetadata> validMetadataInput, List<SIARDDatabaseMetadata> metadataPairs) {
    List<String> malformedMetadataKeys = new ArrayList<>();

    for (SIARDDatabaseMetadata pair : metadataPairs) {
      if (!validDescriptiveMetadataKeys.contains(pair)) {
        if (!validMetadataInput.contains(pair)) {
          malformedMetadataKeys.add(cliInputParameters.get(pair));
        }
      }
    }

    return malformedMetadataKeys;
  }

  private static SIARDDatabaseMetadata parse(String toParse) throws ModuleException {

    SIARDDatabaseMetadata metadata;

    int args = count(Constants.SEPARATOR, toParse);

    Pattern descriptiveMetadataPattern = Pattern
      .compile("^(\\w+)" + Constants.SEPARATOR + "(.*?)" + Constants.SEPARATOR + "$");

    Matcher matcher = descriptiveMetadataPattern.matcher(toParse);

    if (matcher.matches()) {
      if (args != 2) {
        throw new EditDatabaseMetadataParserException("Descriptive metadata receives only two arguments")
          .withFaultyArgument(toParse.replace(Constants.SEPARATOR, " "));
      }
      return new SIARDDatabaseMetadata(matcher.group(1), matcher.group(2));
    }

    Pattern databaseMetadataPattern = Pattern.compile(
      "^(\\w+:.*?" + Constants.SEPARATOR + ")(\\w+" + Constants.SEPARATOR + ")(.*?" + Constants.SEPARATOR + ")$");

    matcher = databaseMetadataPattern.matcher(toParse);

    if (matcher.matches()) { // For now group(2) is only needed to verify that is description the field to
                             // update.
                             // Just added on the pattern for possible future use
      String metadataPath = matcher.group(1);
      String fieldToUpdate = matcher.group(2);
      String value = matcher.group(3);

      if (!fieldToUpdate.equalsIgnoreCase("description" + Constants.SEPARATOR)) {
        throw new EditDatabaseMetadataParserException("Only metadata 'description' can be updated")
          .withFaultyArgument(toParse.replace(Constants.SEPARATOR, " "));
      }

      metadata = decompose(metadataPath, StringUtils.removeEnd(value, Constants.SEPARATOR));
    } else {
      throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
        .withFaultyArgument(toParse.replace(Constants.SEPARATOR, " "));
    }

    return metadata;
  }

  private static SIARDDatabaseMetadata decompose(String metadataPath, String value) throws ModuleException {

    SIARDDatabaseMetadata metadata;
    String normalizedMetadataPath = StringUtils.replace(metadataPath, Constants.SEPARATOR, " ");

    Pattern schemaPattern = Pattern.compile("schema:(.*?)" + Constants.SEPARATOR); // solo
    Pattern tablePattern = Pattern.compile("table:(.*?)" + Constants.SEPARATOR); // schema
    Pattern columnPattern = Pattern.compile("column:(.*?)" + Constants.SEPARATOR); // table | view
    Pattern triggerPattern = Pattern.compile("trigger:(.*?)" + Constants.SEPARATOR); // table
    Pattern viewPattern = Pattern.compile("view:(.*?)" + Constants.SEPARATOR); // schema
    Pattern routinePattern = Pattern.compile("routine:(.*?)" + Constants.SEPARATOR); // schema
    Pattern primaryKeyPattern = Pattern.compile("primaryKey:(.*?)" + Constants.SEPARATOR); // table
    Pattern foreignKeyPattern = Pattern.compile("foreignKey:(.*?)" + Constants.SEPARATOR); // table
    Pattern candidateKeyPattern = Pattern.compile("candidateKey:(.*?)" + Constants.SEPARATOR); // table
    Pattern checkConstraintPattern = Pattern.compile("checkConstraint:(.*?)" + Constants.SEPARATOR); // table
    Pattern parameterPattern = Pattern.compile("parameter:(.*?)" + Constants.SEPARATOR); // routine
    Pattern userPattern = Pattern.compile("user:(.*?)" + Constants.SEPARATOR); // solo
    Pattern rolePattern = Pattern.compile("role:(.*?)" + Constants.SEPARATOR); // solo
    Pattern privilegePattern = Pattern.compile("privilege:(.*?)" + Constants.SEPARATOR); // solo

    HashMap<String, Integer> counters = new HashMap<>();

    counters.put("schema", count(schemaPattern.pattern(), metadataPath));
    counters.put("table", count(tablePattern.pattern(), metadataPath));
    counters.put("column", count(columnPattern.pattern(), metadataPath));
    counters.put("trigger", count(triggerPattern.pattern(), metadataPath));
    counters.put("view", count(viewPattern.pattern(), metadataPath));
    counters.put("primaryKey", count(primaryKeyPattern.pattern(), metadataPath));
    counters.put("foreignKey", count(foreignKeyPattern.pattern(), metadataPath));
    counters.put("candidateKey", count(candidateKeyPattern.pattern(), metadataPath));
    counters.put("parameter", count(parameterPattern.pattern(), metadataPath));
    counters.put("routine", count(routinePattern.pattern(), metadataPath));
    counters.put("checkConstraint", count(checkConstraintPattern.pattern(), metadataPath));
    counters.put("user", count(userPattern.pattern(), metadataPath));
    counters.put("role", count(rolePattern.pattern(), metadataPath));
    counters.put("privilege", count(privilegePattern.pattern(), metadataPath));

    int countAll = counters.get("schema") + counters.get("table") + counters.get("column") + counters.get("trigger")
      + counters.get("view") + counters.get("routine") + counters.get("primaryKey") + counters.get("foreignKey")
      + counters.get("candidateKey") + counters.get("parameter") + counters.get("checkConstraint")
      + counters.get("user") + counters.get("role") + counters.get("privilege");

    int countKey = counters.get("schema") + counters.get("user") + counters.get("role") + counters.get("privilege");

    if (countKey != 1) {
      throw new EditDatabaseMetadataParserException("Missing keyword: schema, user, role, or privilege")
        .withFaultyArgument(normalizedMetadataPath);
    }

    String schemaName = null;
    String tableName = null;
    String viewName = null;
    String routineName = null;

    metadata = new SIARDDatabaseMetadata();

    if (countAll == 1) {
      if (counters.get("schema") == 1 ^ counters.get("user") == 1 ^ counters.get("role") == 1
        ^ counters.get("privilege") == 1) {
        if (counters.get("schema") == 1) {
          Matcher m = schemaPattern.matcher(metadataPath);
          while (m.find()) {
            schemaName = m.group(1);
            if (StringUtils.isBlank(schemaName)) {
              throw new EditDatabaseMetadataParserException("Missing schema name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
            metadata.setValue(value);
          }
          return metadata;
        }
        if (counters.get("user") == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = userPattern.matcher(metadataPath);
          while (m.find()) {
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.USER, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }
        if (counters.get("privilege") == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = privilegePattern.matcher(metadataPath);
          while (m.find()) {
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.PRIVILEGE, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Expecting schema, user, role, or privilege")
          .withFaultyArgument(normalizedMetadataPath);
      }
    } else if (countAll == 2 && counters.get("schema") == 1) {
      Matcher m = schemaPattern.matcher(metadataPath);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }
      if (counters.get("view") == 1 ^ counters.get("table") == 1 ^ counters.get("routine") == 1) {
        if (counters.get("view") == 1) {
          metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = viewPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing view name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }

        if (counters.get("table") == 1) {
          metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = tablePattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing table name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, parsed);
            metadata.setValue(value);
          }
          return metadata;
        } else {
          metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = routinePattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing routine name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE, parsed);
            metadata.setValue(value);
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Missing table, view, or routine")
          .withFaultyArgument(normalizedMetadataPath);
      }
    } else if (countAll == 3 && counters.get("schema") == 1) {
      Matcher m = schemaPattern.matcher(metadataPath);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }
      if (counters.get("table") == 1 && counters.get("routine") == 0 && counters.get("view") == 0) {
        m = tablePattern.matcher(metadataPath);
        while (m.find()) {
          tableName = m.group(1);
          if (StringUtils.isBlank(tableName)) {
            throw new EditDatabaseMetadataParserException("Missing table name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        if (counters.get("column") == 1 ^ counters.get("trigger") == 1 ^ counters.get("primaryKey") == 1
          ^ counters.get("candidateKey") == 1 ^ counters.get("foreignKey") == 1
          ^ counters.get("checkConstraint") == 1) {
          metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
          metadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE, tableName);
          if (counters.get("column") == 1) {
            m = columnPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing column name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.TABLE_COLUMN, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (counters.get("trigger") == 1) {
            m = triggerPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing trigger name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.TRIGGER, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (counters.get("primaryKey") == 1) {
            m = primaryKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing primary key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.PRIMARY_KEY, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (counters.get("candidateKey") == 1) {
            m = candidateKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing candidate key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.CANDIDATE_KEY, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (counters.get("foreignKey") == 1) {
            m = foreignKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing foreign key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.FOREIGN_KEY, parsed);
              metadata.setValue(value);
            }
            return metadata;
          } else {
            m = checkConstraintPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing check constraint name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadata(SIARDDatabaseMetadata.CHECK_CONSTRAINT, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }

      if (counters.get("table") == 1 && (counters.get("routine") == 1 || counters.get("view") == 1)) {
        throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
          .withFaultyArgument(normalizedMetadataPath);
      }

      if (counters.get("table") == 0 && counters.get("routine") == 1 && counters.get("view") == 0) {
        metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = routinePattern.matcher(metadataPath);
        while (m.find()) {
          routineName = m.group(1);
          if (StringUtils.isBlank(routineName)) {
            throw new EditDatabaseMetadataParserException("Missing routine name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        metadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE, routineName);
        if (counters.get("parameter") == 1) {
          m = parameterPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing parameter name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.ROUTINE_PARAMETER, parsed);
            metadata.setValue(value);
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }

      if (counters.get("table") == 0 && counters.get("routine") == 0 && counters.get("view") == 1) {
        metadata.setDatabaseMetadata(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = viewPattern.matcher(metadataPath);
        while (m.find()) {
          viewName = m.group(1);
          if (StringUtils.isBlank(viewName)) {
            throw new EditDatabaseMetadataParserException("Missing parameter name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        metadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW, viewName);
        if (counters.get("column") == 1) {
          m = columnPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing column name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadata(SIARDDatabaseMetadata.VIEW_COLUMN, parsed);
            metadata.setValue(value);
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }
    } else {
      throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
        .withFaultyArgument(normalizedMetadataPath);
    }

    return metadata;
  }

  private DatabaseStructure update(DatabaseStructure dbStructure, List<SIARDDatabaseMetadata> metadataList)
    throws ModuleException {
    for (SIARDDatabaseMetadata metadata : metadataList) {
      switch (metadata.getToUpdate()) {
        case SIARDDatabaseMetadata.SCHEMA:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " description from '"
              + emptyString(dbStructure.getSchemaByName(metadata.getSchema()).getDescription()) + "' to '"
              + metadata.getValue() + "'");

            dbStructure.updateSchemaDescription(metadata.getSchema(), metadata.getValue());

            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " successfully updated",
              "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating schema description", "schema '" + metadata.getSchema() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.ROLE:
          try {
            reporter.metadataUpdated("Changing role:" + metadata.getRole() + " description from '"
              + emptyString(dbStructure.getRoleByName(metadata.getRole()).getDescription()) + "' to '"
              + metadata.getValue() + "'");

            dbStructure.updateRoleDescription(metadata.getRole(), metadata.getValue());
            reporter.metadataUpdated("Metadata for role:" + metadata.getRole() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating role description", "role '" + metadata.getRole() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.USER:
          try {
            reporter.metadataUpdated("Changing user:" + metadata.getUser() + "description from '"
              + emptyString(dbStructure.getUserByName(metadata.getUser()).getDescription()) + "' to '"
              + metadata.getValue() + "'");

            dbStructure.updateUserDescription(metadata.getUser(), metadata.getValue());
            reporter.metadataUpdated("Metadata for user:" + metadata.getUser() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating user description", "user '" + metadata.getUser() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.PRIVILEGE:
          // TODO: Implement
          break;
        case SIARDDatabaseMetadata.TABLE:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " description from '"
              + emptyString(
                dbStructure.getSchemaByName(metadata.getSchema()).getTableByName(metadata.getTable()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateTableDescription(metadata.getSchema(), metadata.getTable(), metadata.getValue());
            reporter.metadataUpdated(
              "Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable() + " successfully updated",
              "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating table description", "table '" + metadata.getTable() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.TABLE_COLUMN:
          try {
            reporter.metadataUpdated("Changing schema:"
              + metadata.getSchema() + " table:" + metadata.getTable() + " column:" + metadata.getTableColumn()
              + " description from '" + emptyString(dbStructure.getSchemaByName(metadata.getSchema())
                .getTableByName(metadata.getTable()).getColumnByName(metadata.getTableColumn()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateTableColumnDescription(metadata.getSchema(), metadata.getTable(),
              metadata.getTableColumn(), metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " column:" + metadata.getTableColumn() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating column description", "column '" + metadata.getTableColumn() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.TRIGGER:
          try {
            reporter.metadataUpdated("Changing schema:"
              + metadata.getSchema() + " table:" + metadata.getTable() + " trigger:" + metadata.getTableColumn()
              + " description from '" + emptyString(dbStructure.getSchemaByName(metadata.getSchema())
                .getTableByName(metadata.getTable()).getTriggerByName(metadata.getTrigger()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateTriggerDescription(metadata.getSchema(), metadata.getTable(), metadata.getTrigger(),
              metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " trigger:" + metadata.getTrigger() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating trigger description", "trigger '" + metadata.getTrigger() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.PRIMARY_KEY:
          try {
            reporter.metadataUpdated("Changing schema:"
              + metadata.getSchema() + " table:" + metadata.getTable() + " primaryKey:" + metadata.getTableColumn()
              + " description from '" + emptyString(dbStructure.getSchemaByName(metadata.getSchema())
                .getTableByName(metadata.getTable()).getPrimaryKey().getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updatePrimaryKeyDescription(metadata.getSchema(), metadata.getTable(), metadata.getPrimaryKey(),
              metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " primaryKey:" + metadata.getPrimaryKey() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating primaryKey description",
              "primaryKey '" + metadata.getPrimaryKey() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.FOREIGN_KEY:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " foreignKey:" + metadata.getTableColumn() + " description from '"
              + emptyString(dbStructure.getSchemaByName(metadata.getSchema()).getTableByName(metadata.getTable())
                .getForeignKeyByName(metadata.getForeignKey()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateForeignKeyDescription(metadata.getSchema(), metadata.getTable(), metadata.getForeignKey(),
              metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " foreignKey:" + metadata.getForeignKey() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating foreignKey description",
              "foreignKey '" + metadata.getForeignKey() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.CANDIDATE_KEY:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " candidateKey:" + metadata.getTableColumn() + " description from '"
              + emptyString(dbStructure.getSchemaByName(metadata.getSchema()).getTableByName(metadata.getTable())
                .getCandidateKeyByName(metadata.getCandidateKey()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateCandidateKeyDescription(metadata.getSchema(), metadata.getTable(),
              metadata.getCandidateKey(), metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " candidateKey:" + metadata.getCandidateKey() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating candidateKey description",
              "candidateKey '" + metadata.getCandidateKey() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.CHECK_CONSTRAINT:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " checkConstraint:" + metadata.getTableColumn() + " description from '"
              + emptyString(dbStructure.getSchemaByName(metadata.getSchema()).getTableByName(metadata.getTable())
                .getCheckConstraintByName(metadata.getCheckConstraint()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateCheckConstraintDescription(metadata.getSchema(), metadata.getTable(),
              metadata.getCheckConstraint(), metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " table:" + metadata.getTable()
              + " checkConstraint:" + metadata.getCheckConstraint() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating checkConstraint description",
              "candidateKey '" + metadata.getCheckConstraint() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.VIEW:
          try {
            reporter.metadataUpdated(
              "Changing schema:" + metadata.getSchema() + " view:" + metadata.getView() + " description from '"
                + emptyString(
                  dbStructure.getSchemaByName(metadata.getSchema()).getViewByName(metadata.getView()).getDescription())
                + "' to '" + metadata.getValue() + "'");

            dbStructure.updateViewDescription(metadata.getSchema(), metadata.getView(), metadata.getValue());
            reporter.metadataUpdated(
              "Metadata for schema:" + metadata.getSchema() + " view:" + metadata.getView() + " successfully updated",
              "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating view description", "view '" + metadata.getView() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.VIEW_COLUMN:
          try {
            reporter.metadataUpdated("Changing schema:"
              + metadata.getSchema() + " view:" + metadata.getView() + "column:" + metadata.getViewColumn()
              + " description from '" + emptyString(dbStructure.getSchemaByName(metadata.getSchema())
                .getViewByName(metadata.getView()).getColumnByName(metadata.getViewColumn()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateViewColumnDescription(metadata.getSchema(), metadata.getView(), metadata.getViewColumn(),
              metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " view:" + metadata.getView()
              + "column:" + metadata.getViewColumn() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating column description", "column '" + metadata.getViewColumn() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.ROUTINE:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " routine:" + metadata.getRoutine()
              + " description from '" + emptyString(dbStructure.getSchemaByName(metadata.getSchema())
                .getRoutineByName(metadata.getRoutine()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateRoutineDescription(metadata.getSchema(), metadata.getRoutine(), metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " routine:" + metadata.getRoutine()
              + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating routine description", "routine '" + metadata.getRoutine() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.ROUTINE_PARAMETER:
          try {
            reporter.metadataUpdated("Changing schema:" + metadata.getSchema() + " routine:" + metadata.getRoutine()
              + "parameter:" + metadata.getRoutineParameter() + " description from '"
              + emptyString(dbStructure.getSchemaByName(metadata.getSchema()).getRoutineByName(metadata.getRoutine())
                .getParameterByName(metadata.getRoutineParameter()).getDescription())
              + "' to '" + metadata.getValue() + "'");

            dbStructure.updateRoutineParameterDescription(metadata.getSchema(), metadata.getRoutine(),
              metadata.getRoutineParameter(), metadata.getValue());
            reporter.metadataUpdated("Metadata for schema:" + metadata.getSchema() + " routine:" + metadata.getRoutine()
              + "parameter:" + metadata.getRoutineParameter() + " successfully updated", "Success: ");
          } catch (NullPointerException e) {
            reporter.failed("updating routine parameter description",
              "parameter '" + metadata.getRoutineParameter() + "' not found");
            throw new ModuleException()
              .withMessage("Error updating the SIARD metadata, please check the log file for more information")
              .withCause(e);
          }
          break;
        case SIARDDatabaseMetadata.SIARD_DBNAME:
          reporter.metadataUpdated(
            "Changing dbname from '" + emptyString(dbStructure.getName()) + "' to '" + metadata.getValue() + "'");
          dbStructure.setName(metadata.getValue());
          reporter.metadataUpdated("Metadata for dbname successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_DESCRIPTION:
          reporter.metadataUpdated("Changing description from '" + emptyString(dbStructure.getDescription()) + "' to '"
            + metadata.getValue() + "'");
          dbStructure.setDescription(metadata.getValue());
          reporter.metadataUpdated("Metadata for description successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVER:
          reporter.metadataUpdated(
            "Changing archiver from '" + emptyString(dbStructure.getArchiver()) + "' to '" + metadata.getValue() + "'");
          dbStructure.setArchiver(metadata.getValue());
          reporter.metadataUpdated("Metadata for archive successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVER_CONTACT:
          reporter.metadataUpdated("Changing archiverContact from '" + emptyString(dbStructure.getArchiverContact())
            + "' to '" + metadata.getValue() + "'");
          dbStructure.setArchiverContact(metadata.getValue());
          reporter.metadataUpdated("Metadata for archiverContact successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_DATA_OWNER:
          reporter.metadataUpdated("Changing dataOwner from '" + emptyString(dbStructure.getDataOwner()) + "' to '"
            + metadata.getValue() + "'");
          dbStructure.setDataOwner(metadata.getValue());
          reporter.metadataUpdated("Metadata for dataOwner successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_DATA_ORIGIN_TIMESPAN:
          reporter.metadataUpdated("Changing dataOriginTimespan from '"
            + emptyString(dbStructure.getDataOriginTimespan()) + "' to '" + metadata.getValue() + "'");
          dbStructure.setDataOriginTimespan(metadata.getValue());
          reporter.metadataUpdated("Metadata for dataOriginTimespan successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_PRODUCER_APPLICATION:
          reporter.metadataUpdated("Changing producerApplication from '"
            + emptyString(dbStructure.getProducerApplication()) + "' to '" + metadata.getValue() + "'");
          dbStructure.setProducerApplication(metadata.getValue());
          reporter.metadataUpdated("Metadata for producerApplication successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVAL_DATE:
          reporter.metadataUpdated("Changing archivalDate from '" + emptyString(dbStructure.getArchivalDate())
            + "' to '" + metadata.getValue() + "'");
          dbStructure.setArchivalDate(JodaUtils.xsDateParse(metadata.getValue()));
          reporter.metadataUpdated("Metadata for archivalDate successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_MESSAGE_DIGEST:
          // TODO: implement message digest
          break;
        case SIARDDatabaseMetadata.SIARD_CLIENT_MACHINE:
          reporter.metadataUpdated("Changing clientMachine from '" + emptyString(dbStructure.getClientMachine())
            + "' to '" + metadata.getValue() + "'");
          dbStructure.setClientMachine(metadata.getValue());
          reporter.metadataUpdated("Metadata for clientMachine successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_DATABASE_PRODUCT:
          reporter.metadataUpdated("Changing databaseProduct from '" + emptyString(dbStructure.getProductName())
            + "' to '" + metadata.getValue() + "'");
          dbStructure.setProductName(metadata.getValue());
          reporter.metadataUpdated("Metadata for databaseProduct successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_CONNECTION:
          reporter.metadataUpdated(
            "Changing connection from '" + emptyString(dbStructure.getUrl()) + "' to '" + metadata.getValue() + "'");
          dbStructure.setUrl(metadata.getValue());
          reporter.metadataUpdated("Metadata for connection successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.SIARD_DATABASE_USER:
          reporter.metadataUpdated("Changing databaseUser from '" + emptyString(dbStructure.getDatabaseUser())
            + "' to '" + metadata.getValue() + "'");
          dbStructure.setDatabaseUser(metadata.getValue());
          reporter.metadataUpdated("Metadata for databaseUser successfully updated", "Success: ");
          break;
        case SIARDDatabaseMetadata.NONE:
        default:
          return dbStructure;
      }
    }

    return dbStructure;

  }

  private static String emptyString(String input) {
    if (StringUtils.isBlank(input))
      return "";
    else
      return input;
  }

  private static String emptyString(DateTime input) {
    String s = JodaUtils.xsDateFormat(input);
    if (StringUtils.isBlank(s))
      return "";
    else
      return s;
  }

  private static int count(String pattern, String input) {
    return (input.split(pattern, -1).length - 1);
  }

}
