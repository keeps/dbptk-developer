/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
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
   *
   *
   * @throws ModuleException
   */
  public void list() throws ModuleException {
    HashMap<Parameter, String> importParameters = buildImportParameters(editModuleParameters, editModuleFactory);

    EditModule editModule = editModuleFactory.buildModule(importParameters, reporter);
    editModule.setOnceReporter(reporter);

    DatabaseStructure metadata = editModule.getMetadata();

    PrintUtils.printDatabaseStructureTree(metadata, System.out);
  }

  /**
   * Edits the {@link DatabaseStructure} according to the key value pairs received
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

  // auxiliary internal methods

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
    List<SIARDDatabaseMetadata> validMetadataInput, List<SIARDDatabaseMetadata> metadataPairs) throws ModuleException {
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

    Pattern descriptiveMetadataPattern = Pattern
      .compile("(\\w+)" + Constants.SEPARATOR + "(.*?)" + Constants.SEPARATOR);

    Matcher matcher = descriptiveMetadataPattern.matcher(toParse);

    if (matcher.matches()) {
      return new SIARDDatabaseMetadata(matcher.group(1), matcher.group(2));
    }

    Pattern databaseMetadataPattern = Pattern.compile(
      "^(\\w+:.*?" + Constants.SEPARATOR + ")(\\w+" + Constants.SEPARATOR + ")(.*?" + Constants.SEPARATOR + ")$");

    matcher = databaseMetadataPattern.matcher(toParse);

    if (matcher.matches()) { // For now group(2) is only needed to verify that is description the field to update.
                             // Just added on the pattern for possible future use
      String metadataPath = matcher.group(1);
      String fieldToUpdate = matcher.group(2);
      String value = matcher.group(3);

      if (!fieldToUpdate.equalsIgnoreCase("description" + Constants.SEPARATOR)) {
        throw new EditDatabaseMetadataParserException("Only metadata 'description' can be updated")
            .withFaultyArgument(toParse.replace(Constants.SEPARATOR, " "));
      }

      metadata = get(metadataPath, StringUtils.removeEnd(value, Constants.SEPARATOR));
    } else {
      throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
        .withFaultyArgument(toParse.replace(Constants.SEPARATOR, " "));
    }

    return metadata;
  }

  private static SIARDDatabaseMetadata get(String metadataPath, String value) throws ModuleException {

    SIARDDatabaseMetadata metadata = null;
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

    int schemaCount = count(schemaPattern.pattern(), metadataPath);
    int tableCount = count(tablePattern.pattern(), metadataPath);
    int columnCount = count(columnPattern.pattern(), metadataPath);
    int triggerCount = count(triggerPattern.pattern(), metadataPath);
    int viewCount = count(viewPattern.pattern(), metadataPath);
    int routineCount = count(routinePattern.pattern(), metadataPath);
    int primaryKeyCount = count(primaryKeyPattern.pattern(), metadataPath);
    int foreignKeyCount = count(foreignKeyPattern.pattern(), metadataPath);
    int candidateKeyCount = count(candidateKeyPattern.pattern(), metadataPath);
    int parameterCount = count(parameterPattern.pattern(), metadataPath);
    int constraintCount = count(checkConstraintPattern.pattern(), metadataPath);
    int countUser = count(userPattern.pattern(), metadataPath);
    int countRole = count(rolePattern.pattern(), metadataPath);
    int countPrivilege = count(privilegePattern.pattern(), metadataPath);

    int countAll = schemaCount + tableCount + columnCount + triggerCount + viewCount + routineCount + primaryKeyCount
      + foreignKeyCount + candidateKeyCount + parameterCount + constraintCount + countUser + countRole + countPrivilege;

    int countKey = schemaCount + countUser + countRole + countPrivilege;

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
      if (schemaCount == 1 ^ countUser == 1 ^ countRole == 1 ^ countPrivilege == 1) {
        if (schemaCount == 1) {
          Matcher m = schemaPattern.matcher(metadataPath);
          while (m.find()) {
            schemaName = m.group(1);
            if (StringUtils.isBlank(schemaName)) {
              throw new EditDatabaseMetadataParserException("Missing schema name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
            metadata.setValue(value);
          }
          return metadata;
        }
        if (countUser == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = userPattern.matcher(metadataPath);
          while (m.find()) {
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.USER, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }
        if (countPrivilege == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = privilegePattern.matcher(metadataPath);
          while (m.find()) {
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.PRIVILEGE, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Expecting schema, user, role, or privilege")
          .withFaultyArgument(normalizedMetadataPath);
      }
    } else if (countAll == 2 && schemaCount == 1) {
      Matcher m = schemaPattern.matcher(metadataPath);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }
      if (viewCount == 1 ^ tableCount == 1 ^ routineCount == 1) {
        if (viewCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = viewPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing view name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW, m.group(1));
            metadata.setValue(value);
          }
          return metadata;
        }

        if (tableCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = tablePattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing table name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE, parsed);
            metadata.setValue(value);
          }
          return metadata;
        } else {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = routinePattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing routine name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE, parsed);
            metadata.setValue(value);
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Missing table, view, or routine")
          .withFaultyArgument(normalizedMetadataPath);
      }
    } else if (countAll == 3 && schemaCount == 1) {
      Matcher m = schemaPattern.matcher(metadataPath);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }
      if (tableCount == 1 && routineCount == 0 && viewCount == 0) {
        m = tablePattern.matcher(metadataPath);
        while (m.find()) {
          tableName = m.group(1);
          if (StringUtils.isBlank(tableName)) {
            throw new EditDatabaseMetadataParserException("Missing table name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        if (columnCount == 1 ^ triggerCount == 1 ^ primaryKeyCount == 1 ^ candidateKeyCount == 1 ^ foreignKeyCount == 1
          ^ constraintCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE, tableName);
          if (columnCount == 1) {
            m = columnPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing column name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE_COLUMN, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (triggerCount == 1) {
            m = triggerPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing trigger name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TRIGGER, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (primaryKeyCount == 1) {
            m = primaryKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing primary key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.PRIMARY_KEY, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (candidateKeyCount == 1) {
            m = candidateKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing candidate key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CANDIDATE_KEY, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
          if (foreignKeyCount == 1) {
            m = foreignKeyPattern.matcher(metadataPath);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing foreign key name")
                  .withFaultyArgument(normalizedMetadataPath);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.FOREIGN_KEY, parsed);
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
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CHECK_CONSTRAINT, parsed);
              metadata.setValue(value);
            }
            return metadata;
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }

      if (tableCount == 1 && (routineCount == 1 || viewCount == 1)) {
        throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
          .withFaultyArgument(normalizedMetadataPath);
      }

      if (tableCount == 0 && routineCount == 1 && viewCount == 0) {
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = routinePattern.matcher(metadataPath);
        while (m.find()) {
          routineName = m.group(1);
          if (StringUtils.isBlank(routineName)) {
            throw new EditDatabaseMetadataParserException("Missing routine name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE, routineName);
        if (parameterCount == 1) {
          m = parameterPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing parameter name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE_PARAMETER, parsed);
            metadata.setValue(value);
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(normalizedMetadataPath);
        }
      }

      if (tableCount == 0 && routineCount == 0 && viewCount == 1) {
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = viewPattern.matcher(metadataPath);
        while (m.find()) {
          viewName = m.group(1);
          if (StringUtils.isBlank(viewName)) {
            throw new EditDatabaseMetadataParserException("Missing parameter name")
              .withFaultyArgument(normalizedMetadataPath);
          }
        }
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW, viewName);
        if (columnCount == 1) {
          m = columnPattern.matcher(metadataPath);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing column name")
                .withFaultyArgument(normalizedMetadataPath);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW_COLUMN, parsed);
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

  private static int count(String pattern, String input) {
    return (input.split(pattern, -1).length - 1);
  }

  private static DatabaseStructure update(DatabaseStructure dbStructure, List<SIARDDatabaseMetadata> metadataList) {

    for (SIARDDatabaseMetadata metadata : metadataList) {
      switch (metadata.getToUpdate()) {
        case SIARDDatabaseMetadata.SCHEMA:
          dbStructure.updateSchemaDescription(metadata.getSchema(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.ROLE:
          dbStructure.updateRoleDescription(metadata.getRole(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.USER:
          dbStructure.updateUserDescription(metadata.getUser(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.PRIVILEGE:
          // TODO: Implement
          break;
        case SIARDDatabaseMetadata.TABLE:
          dbStructure.updateTableDescription(metadata.getSchema(), metadata.getTable(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.TABLE_COLUMN:
          dbStructure.updateTableColumnDescription(metadata.getSchema(), metadata.getTable(), metadata.getTableColumn(),
            metadata.getValue());
          break;
        case SIARDDatabaseMetadata.TRIGGER:
          dbStructure.updateTriggerDescription(metadata.getSchema(), metadata.getTable(), metadata.getTrigger(),
            metadata.getValue());
          break;
        case SIARDDatabaseMetadata.PRIMARY_KEY:
          dbStructure.updatePrimaryKeyDescription(metadata.getSchema(), metadata.getTable(), metadata.getPrimaryKey(),
            metadata.getValue());
          break;
        case SIARDDatabaseMetadata.FOREIGN_KEY:
          dbStructure.updateForeignKeyDescription(metadata.getSchema(), metadata.getTable(), metadata.getForeignKey(),
            metadata.getValue());
          break;
        case SIARDDatabaseMetadata.CANDIDATE_KEY:
          dbStructure.updateCandidateKeyDescription(metadata.getSchema(), metadata.getTable(),
            metadata.getCandidateKey(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.CHECK_CONSTRAINT:
          dbStructure.updateCheckConstraintDescription(metadata.getSchema(), metadata.getTable(),
            metadata.getCheckConstraint(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.VIEW:
          dbStructure.updateViewDescription(metadata.getSchema(), metadata.getView(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.VIEW_COLUMN:
          dbStructure.updateViewColumnDescription(metadata.getSchema(), metadata.getView(), metadata.getViewColumn(),
            metadata.getValue());
          break;
        case SIARDDatabaseMetadata.ROUTINE:
          dbStructure.updateRoutineDescription(metadata.getSchema(), metadata.getRoutine(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.ROUTINE_PARAMETER:
          dbStructure.updateRoutineParameterDescription(metadata.getSchema(), metadata.getRoutine(),
            metadata.getRoutineParameter(), metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DBNAME:
          dbStructure.setName(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DESCRIPTION:
          dbStructure.setDescription(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVER:
          dbStructure.setArchiver(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVER_CONTACT:
          dbStructure.setArchiverContact(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DATA_OWNER:
          dbStructure.setDataOwner(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DATA_ORIGIN_TIMESPAN:
          dbStructure.setDataOriginTimespan(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_PRODUCER_APPLICATION:
          dbStructure.setProducerApplication(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_ARCHIVAL_DATE:
          dbStructure.setArchivalDate(JodaUtils.xsDateParse(metadata.getValue()));
          break;
        case SIARDDatabaseMetadata.SIARD_MESSAGE_DIGEST:
          // TODO: implement message digest
          break;
        case SIARDDatabaseMetadata.SIARD_CLIENT_MACHINE:
          dbStructure.setClientMachine(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DATABASE_PRODUCT:
          dbStructure.setProductName(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_CONNECTION:
          dbStructure.setUrl(metadata.getValue());
          break;
        case SIARDDatabaseMetadata.SIARD_DATABASE_USER:
          dbStructure.setDatabaseUser(metadata.getValue());
          break;
        default:
          ;
      }
    }

    return dbStructure;
  }
}
