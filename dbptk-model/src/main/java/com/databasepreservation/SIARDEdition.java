/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEdition {
  private EditModuleFactory editModuleFactory;
  private HashMap<Parameter, List<String>> editModuleParameters;
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

    List<SIARDDatabaseMetadata> SIARDDatabaseMetadataKeys = editModule.getDatabaseMetadataKeys();

  }

  /**
   * Edits the {@link DatabaseStructure} according to the key value pairs received
   *
   * @throws EditDatabaseMetadataParserException
   *           The metadata path is not a valid one
   * @throws ModuleException
   *           Generic module exception
   *
   */
  public void edit() throws ModuleException {

    HashMap<Parameter, String> importParameters = buildImportParameters(editModuleParameters, editModuleFactory);
    HashMap<String, String> metadataPairs = buildMetadataPairs(editModuleParameters, editModuleFactory);

    EditModule editModule = editModuleFactory.buildModule(importParameters, reporter);
    editModule.setOnceReporter(reporter);

    // COMES FROM THE XSD FILE
    List<String> descriptiveMetadataKeys = editModule.getDescriptiveSIARDMetadataKeys();
    // COMES FROM THE XML FILE
    List<SIARDDatabaseMetadata> SIARDDatabaseMetadataKeys = editModule.getDatabaseMetadataKeys();

    List<String> malformedMetadataKeys = validateMetadataKeys(descriptiveMetadataKeys, SIARDDatabaseMetadataKeys,
      metadataPairs.keySet());

    if (malformedMetadataKeys.isEmpty()) {
      List<SIARDDatabaseMetadata> siardDatabaseMetadata = retrieveMetadataValuesFromCLI(metadataPairs,
        descriptiveMetadataKeys);

      DatabaseStructure metadata = editModule.getMetadata();

      DatabaseStructure updated = update(metadata, siardDatabaseMetadata);
      editModule.saveMetadata(updated);
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
        if (p != null && entry.getKey().equals(p)) {
          if (p.longName().contentEquals("file")) {
            importParameters.put(p, entry.getValue().get(0));
          }
        }
      }
    }

    return importParameters;
  }

  private static HashMap<String, String> buildMetadataPairs(HashMap<Parameter, List<String>> editModuleParameters,
    EditModuleFactory editModuleFactory) {

    HashMap<String, String> pairs = new HashMap<>();

    for (Map.Entry<Parameter, List<String>> entry : editModuleParameters.entrySet()) {
      for (Parameter p : editModuleFactory.getSetParameters().keySet()) {
        if (p != null && entry.getKey().equals(p)) {
          List<String> listPairs = entry.getValue();
          for (String pair : listPairs) {
            String[] split = pair.split(Constants.SEPARATOR);
            String key = split[0];
            String value = split[1];

            pairs.put(key, value);
          }
        }
      }
    }

    return pairs;
  }

  private static List<String> validateMetadataKeys(List<String> validDescriptiveMetadataKeys,
    List<SIARDDatabaseMetadata> validMetadataInput, Set<String> metadataKeys) throws ModuleException {
    List<String> malformedMetadataKeys = new ArrayList<>();

    for (String key : metadataKeys) {
      if (!validDescriptiveMetadataKeys.contains(key)) {
        SIARDDatabaseMetadata parsedKey = parse(key);
        if (!validMetadataInput.contains(parsedKey)) {
          malformedMetadataKeys.add(key);
        }
      }
    }

    return malformedMetadataKeys;
  }

  private static List<SIARDDatabaseMetadata> retrieveMetadataValuesFromCLI(HashMap<String, String> metadata,
    List<String> descriptiveMetadataKeys) throws ModuleException {

    ArrayList<SIARDDatabaseMetadata> siardDatabaseMetadata = new ArrayList<>();

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      if (descriptiveMetadataKeys.contains(entry.getKey())) {
        SIARDDatabaseMetadata dbMetadata = new SIARDDatabaseMetadata();
        dbMetadata.setDescriptiveMetadata(entry.getKey());
        dbMetadata.setToUpdate(SIARDDatabaseMetadata.getSIARDMetadataType(entry.getKey()));
        dbMetadata.setValue(entry.getValue());
        siardDatabaseMetadata.add(dbMetadata);
      } else {
        SIARDDatabaseMetadata dbMetadata = parse(entry.getKey());
        if (dbMetadata != null) {
          dbMetadata.setValue(entry.getValue());
          siardDatabaseMetadata.add(dbMetadata);
        }
      }
    }

    return siardDatabaseMetadata;
  }

  private static SIARDDatabaseMetadata parse(String toParse) throws ModuleException {

    SIARDDatabaseMetadata metadata = null;

    int spaces = count("'\\s+[^']", toParse);

    Pattern schemaPattern = Pattern.compile("schema:'(.*?)'"); // solo
    Pattern tablePattern = Pattern.compile("table:'(.*?)'"); // schema
    Pattern columnPattern = Pattern.compile("column:'(.*?)'"); // table | view
    Pattern triggerPattern = Pattern.compile("trigger:'(.*?)'"); // table
    Pattern viewPattern = Pattern.compile("view:'(.*?)'"); // schema
    Pattern routinePattern = Pattern.compile("routine:'(.*?)'"); // schema
    Pattern primaryKeyPattern = Pattern.compile("primaryKey:'(.*?)'"); // table
    Pattern foreignKeyPattern = Pattern.compile("foreignKey:'(.*?)'"); // table
    Pattern candidateKeyPattern = Pattern.compile("candidateKey:'(.*?)'"); // table
    Pattern checkConstraintPattern = Pattern.compile("checkConstraint:'(.*?)'"); // table
    Pattern parameterPattern = Pattern.compile("parameter:'(.*?)'"); // routine
    Pattern userPattern = Pattern.compile("user:'(.*?)'"); // solo
    Pattern rolePattern = Pattern.compile("role:'(.*?)'"); // solo
    Pattern privilegePattern = Pattern.compile("privilege:'(.*?)'"); // solo

    int schemaCount = count(schemaPattern.pattern(), toParse);
    int tableCount = count(tablePattern.pattern(), toParse);
    int columnCount = count(columnPattern.pattern(), toParse);
    int triggerCount = count(triggerPattern.pattern(), toParse);
    int viewCount = count(viewPattern.pattern(), toParse);
    int routineCount = count(routinePattern.pattern(), toParse);
    int primaryKeyCount = count(primaryKeyPattern.pattern(), toParse);
    int foreignKeyCount = count(foreignKeyPattern.pattern(), toParse);
    int candidateKeyCount = count(candidateKeyPattern.pattern(), toParse);
    int parameterCount = count(parameterPattern.pattern(), toParse);
    int constraintCount = count(checkConstraintPattern.pattern(), toParse);
    int countUser = count(userPattern.pattern(), toParse);
    int countRole = count(rolePattern.pattern(), toParse);
    int countPrivilege = count(privilegePattern.pattern(), toParse);

    int countAll = schemaCount + tableCount + columnCount + triggerCount + viewCount + routineCount + primaryKeyCount
      + foreignKeyCount + candidateKeyCount + parameterCount + constraintCount + countUser + countRole + countPrivilege;

    int countKey = schemaCount + countUser + countRole + countPrivilege;

    if (countAll != (spaces + 1)) {
      throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
        .withFaultyArgument(toParse);
    }

    if (countKey != 1) {
      throw new EditDatabaseMetadataParserException("Missing keyword: schema, user, role, or privilege")
        .withFaultyArgument(toParse);
    }

    String schemaName = null;
    String tableName = null;
    String viewName = null;
    String routineName = null;

    metadata = new SIARDDatabaseMetadata();

    if (countAll == 1) {
      if (schemaCount == 1 ^ countUser == 1 ^ countRole == 1 ^ countPrivilege == 1) {
        if (schemaCount == 1) {
          Matcher m = schemaPattern.matcher(toParse);
          while (m.find()) {
            schemaName = m.group(1);
            if (StringUtils.isBlank(schemaName)) {
              throw new EditDatabaseMetadataParserException("Missing schema name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          }
          return metadata;
        }
        if (countUser == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = userPattern.matcher(toParse);
          while (m.find()) {
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.USER, m.group(1));
          }
          return metadata;
        }
        if (countPrivilege == 1) {
          metadata = new SIARDDatabaseMetadata();
          Matcher m = privilegePattern.matcher(toParse);
          while (m.find()) {
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.PRIVILEGE, m.group(1));
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Expecting schema, user, role, or privilege")
          .withFaultyArgument(toParse);
      }
    } else if (countAll == 2 && schemaCount == 1) {
      Matcher m = schemaPattern.matcher(toParse);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name").withFaultyArgument(toParse);
        }
      }
      if (viewCount == 1 ^ tableCount == 1 ^ routineCount == 1) {
        if (viewCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = viewPattern.matcher(toParse);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing view name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW, m.group(1));
          }
          return metadata;
        }

        if (tableCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = tablePattern.matcher(toParse);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing table name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE, parsed);
          }
          return metadata;
        } else {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          m = routinePattern.matcher(toParse);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing routine name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE, parsed);
          }
          return metadata;
        }
      } else {
        throw new EditDatabaseMetadataParserException("Missing table, view, or routine").withFaultyArgument(toParse);
      }
    } else if (countAll == 3 && schemaCount == 1) {
      Matcher m = schemaPattern.matcher(toParse);
      while (m.find()) {
        schemaName = m.group(1);
        if (StringUtils.isBlank(schemaName)) {
          throw new EditDatabaseMetadataParserException("Missing schema name").withFaultyArgument(toParse);
        }
      }
      if (tableCount == 1 && routineCount == 0 && viewCount == 0) {
        m = tablePattern.matcher(toParse);
        while (m.find()) {
          tableName = m.group(1);
          if (StringUtils.isBlank(tableName)) {
            throw new EditDatabaseMetadataParserException("Missing table name").withFaultyArgument(toParse);
          }
        }
        if (columnCount == 1 ^ triggerCount == 1 ^ primaryKeyCount == 1 ^ candidateKeyCount == 1 ^ foreignKeyCount == 1
          ^ constraintCount == 1) {
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
          metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE, tableName);
          if (columnCount == 1) {
            m = columnPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing column name").withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TABLE_COLUMN, parsed);
            }
            return metadata;
          }
          if (triggerCount == 1) {
            m = triggerPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing trigger name").withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.TRIGGER, parsed);
            }
            return metadata;
          }
          if (primaryKeyCount == 1) {
            m = primaryKeyPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing primary key name").withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.PRIMARY_KEY, parsed);
            }
            return metadata;
          }
          if (candidateKeyCount == 1) {
            m = candidateKeyPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing candidate key name").withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CANDIDATE_KEY, parsed);
            }
            return metadata;
          }
          if (foreignKeyCount == 1) {
            m = foreignKeyPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing foreign key name").withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.FOREIGN_KEY, parsed);
            }
            return metadata;
          } else {
            m = checkConstraintPattern.matcher(toParse);
            while (m.find()) {
              String parsed = m.group(1);
              if (StringUtils.isBlank(parsed)) {
                throw new EditDatabaseMetadataParserException("Missing check constraint name")
                  .withFaultyArgument(toParse);
              }
              metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.CHECK_CONSTRAINT, parsed);
            }
            return metadata;
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(toParse);
        }
      }

      if (tableCount == 1 && (routineCount == 1 || viewCount == 1)) {
        throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
          .withFaultyArgument(toParse);
      }

      if (tableCount == 0 && routineCount == 1 && viewCount == 0) {
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = routinePattern.matcher(toParse);
        while (m.find()) {
          routineName = m.group(1);
          if (StringUtils.isBlank(routineName)) {
            throw new EditDatabaseMetadataParserException("Missing routine name").withFaultyArgument(toParse);
          }
        }
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE, routineName);
        if (parameterCount == 1) {
          m = parameterPattern.matcher(toParse);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing parameter name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.ROUTINE_PARAMETER, parsed);
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(toParse);
        }
      }

      if (tableCount == 0 && routineCount == 0 && viewCount == 1) {
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.SCHEMA, schemaName);
        m = viewPattern.matcher(toParse);
        while (m.find()) {
          viewName = m.group(1);
          if (StringUtils.isBlank(viewName)) {
            throw new EditDatabaseMetadataParserException("Missing parameter name").withFaultyArgument(toParse);
          }
        }
        metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW, viewName);
        if (columnCount == 1) {
          m = columnPattern.matcher(toParse);
          while (m.find()) {
            String parsed = m.group(1);
            if (StringUtils.isBlank(parsed)) {
              throw new EditDatabaseMetadataParserException("Missing column name").withFaultyArgument(toParse);
            }
            metadata.setDatabaseMetadataKey(SIARDDatabaseMetadata.VIEW_COLUMN, parsed);
          }
        } else {
          throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
            .withFaultyArgument(toParse);
        }
      }
    } else {
      throw new EditDatabaseMetadataParserException("The metadata path is poorly constructed")
        .withFaultyArgument(toParse);
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
