package com.databasepreservation.modules.config;

import static com.databasepreservation.modules.config.Normalize1NFConfiguration.NormalizedColumnType.ARRAY;
import static com.databasepreservation.modules.config.Normalize1NFConfiguration.NormalizedColumnType.JSON;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.Constants;
import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.*;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.utils.MapUtils;
import com.databasepreservation.utils.ModuleConfigurationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * @author Daniel Lundsgaard Skovenborg <daniel.lundsgaard.skovenborg@stil.dk>
 */
public class Normalize1NFConfiguration extends ImportConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(Normalize1NFConfiguration.class);

  // Value to use for query in merge file to exclude from normalization view
  // creation. Empty string will not work because Constants.EMPTY will be assigned
  // if "query" key is left out to
  // override other fields.
  private static final String EXCLUDE_CUSTOM_VIEW_QUERY = "--"; //
  private static final String DEFAULT_ARRAY_NAME_PATTERN = "${table}__${column}";
  private static final String DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN = "${table}_${column}";
  private static final String DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN = "array_index";
  private static final String DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN = "${column}_item";
  private static final String DEFAULT_ARRAY_TABLE_ALIAS = "a";

  private static final String DEFAULT_JSON_NAME_PATTERN = DEFAULT_ARRAY_NAME_PATTERN;
  private static final String DEFAULT_JSON_FOREIGN_KEY_COLUMN_PATTERN = DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN;

  // TODO: Allow overriding all patterns.
  private String foreignIdColumnDescriptionPattern;

  private String arrayNamePattern = DEFAULT_ARRAY_NAME_PATTERN;
  private String arrayForeignKeyColumnPattern = DEFAULT_ARRAY_FOREIGN_KEY_COLUMN_PATTERN;
  private String arrayDescriptionPattern;
  private String arrayIndexColumnDescriptionPattern;
  private String arrayItemColumnDescriptionPattern;
  private String arrayIndexColumnNamePattern = DEFAULT_ARRAY_INDEX_COLUMN_NAME_PATTERN;
  private String arrayItemColumnNamePattern = DEFAULT_ARRAY_ITEM_COLUMN_NAME_PATTERN;
  private String arrayTableAlias = DEFAULT_ARRAY_TABLE_ALIAS;

  private String jsonNamePattern = DEFAULT_JSON_NAME_PATTERN;
  private String jsonForeignKeyColumnPattern = DEFAULT_JSON_FOREIGN_KEY_COLUMN_PATTERN;
  private String jsonDescriptionPattern;

  private final ModuleConfiguration mergeConfiguration;
  private final boolean noSQLQuotes;

  public Normalize1NFConfiguration(Path outputFile, Path mergeFile, boolean noSQLQuotes, String arrayDescriptionPattern,
    String jsonDescriptionPattern, String foreignIdColumnDescriptionPattern, String arrayIndexColumnDescriptionPattern,
    String arrayItemColumnDescriptionPattern)
    throws ModuleException {
    super(outputFile);
    this.noSQLQuotes = noSQLQuotes;
    this.arrayDescriptionPattern = arrayDescriptionPattern;
    this.jsonDescriptionPattern = jsonDescriptionPattern;
    this.foreignIdColumnDescriptionPattern = foreignIdColumnDescriptionPattern;
    this.arrayIndexColumnDescriptionPattern = arrayIndexColumnDescriptionPattern;
    this.arrayItemColumnDescriptionPattern = arrayItemColumnDescriptionPattern;

    if (mergeFile == null) {
      // Create empty configuration so that we don't have to check for null everywhere.
      mergeConfiguration = new ModuleConfiguration();
    } else {
      try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mergeConfiguration = mapper.readValue(mergeFile.toFile(), ModuleConfiguration.class);
      } catch (IOException e) {
        throw new ModuleException()
          .withMessage("Could not read the merge configuration from file " + mergeFile.normalize().toAbsolutePath())
          .withCause(e);
      }
    }
  }

  @Override
  public void initDatabase() {
    super.initDatabase();

    ModuleConfiguration dbConfiguration = ModuleConfigurationManager.getInstance().getModuleConfiguration();
    Map<DatabaseTechnicalFeatures, Boolean> ignore = dbConfiguration.getIgnore();
    ignore.put(DatabaseTechnicalFeatures.PRIMARY_KEYS, false);
    dbConfiguration.setIgnore(ignore);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    super.handleDataOpenTable(tableId);

    currentTable.getColumns().forEach(this::handleColumn);

    // TODO: add support for using merge configuration to add foreign keys to table,
    // e.g., foreign key from an enum column to a code table constructed with a
    // custom view in the merge configuration.
  }

  private void handleColumn(ColumnStructure column) {

    boolean isArray = column.getType().getSql99TypeName().endsWith("ARRAY");
    boolean isJson = column.getType().getOriginalTypeName().equalsIgnoreCase("json")
      || column.getType().getOriginalTypeName().equalsIgnoreCase("jsonb");

    if (!isArray && !isJson)
      return;

    String schemaName = currentSchema.getName();
    String tableName = currentTable.getName();

    NormalizedColumnType ncType = isArray ? ARRAY : JSON;
    String columnName = column.getName();
    String viewName = formatTblCol(ncType == ARRAY ? arrayNamePattern : jsonNamePattern, tableName, columnName);

    // Remove normalized column from table configuration.
    removeColumnFromConfiguration(schemaName, tableName, columnName);

    // Allow overriding creation of view (e.g., if making a manual normalization).
    CustomViewConfiguration merge = mergeConfiguration.getCustomViewConfiguration(schemaName, viewName);
    if (merge != null && EXCLUDE_CUSTOM_VIEW_QUERY.equals(merge.getQuery())) {
      LOGGER.info("Normalization of {}.{}.{} ({}) is excluded by merge file", schemaName, tableName, columnName,
        viewName);
      return;
    }

    addNormalizationViewConfiguration(ncType, schemaName, tableName, columnName, viewName);
    mergeCustomViewConfiguration(schemaName, viewName);
  }

  private void removeColumnFromConfiguration(String schemaName, String tableName, String columnName) {
    // Assume no copy.
    LOGGER.info("Removing non-1NF column {}.{}.{} from configuration", schemaName, tableName, columnName);
    TableConfiguration tableConfiguration = moduleConfiguration.getTableConfiguration(schemaName, tableName);
    List<ColumnConfiguration> newColumns = tableConfiguration.getColumns().stream()
      .filter(c -> !c.getName().equals(columnName)).collect(Collectors.toList());
    tableConfiguration.setColumns(newColumns);
  }

  private void addNormalizationViewConfiguration(NormalizedColumnType ncType, String schemaName, String tableName,
    String columnName, String viewName) {

    PrimaryKey primaryKey = currentTable.getPrimaryKey();

    if (primaryKey == null) {
      LOGGER.warn("Table {}.{} has no primary key. Cannot create normalization of {} column {}", ncType, schemaName,
        tableName, columnName);
      return;
    }

    LOGGER.info("Creating normalization view of {} column {}.{}.{}", ncType, schemaName, tableName, columnName);

    String description = formatTblCol(ncType == ARRAY ? arrayDescriptionPattern : jsonDescriptionPattern, tableName,
      columnName);
    List<CustomColumnConfiguration> columns = new ArrayList<>();
    String query = ncType == ARRAY ? getArrayNormalizationSQL(schemaName, tableName, columnName, primaryKey, columns)
      : getJsonNormalizationSQL(schemaName, tableName, primaryKey, columns);
    PrimaryKeyConfiguration primaryKeyConfiguration = getPrimaryKeyConfiguration(ncType, primaryKey, tableName,
      columnName);
    ForeignKeyConfiguration foreignKeyConfiguration = getForeignKeyConfiguration(ncType, primaryKey, tableName);

    ModuleConfigurationUtils.addCustomViewConfiguration(moduleConfiguration, schemaName, viewName, true, description,
      query, columns, primaryKeyConfiguration, Collections.singletonList(foreignKeyConfiguration));
  }

  @Override
  public void finishDatabase() throws ModuleException {
    // Add all custom views from the merge configuration if not present.
    moduleConfiguration.getSchemaConfigurations().forEach((schemaName, schemaConfiguration) -> {
      SchemaConfiguration schemaToMerge = mergeConfiguration.getSchemaConfigurations().get(schemaName);
      if (schemaToMerge == null)
        return;

      List<CustomViewConfiguration> customViewConfigurations = schemaConfiguration.getCustomViewConfigurations();

      for (CustomViewConfiguration custom : schemaToMerge.getCustomViewConfigurations()) {
        if (!EXCLUDE_CUSTOM_VIEW_QUERY.equals(custom.getQuery())
          && moduleConfiguration.getCustomViewConfiguration(schemaName, custom.getName()) == null) {
          LOGGER.info("Adding custom view {}.{} from merge configuration file", schemaName, custom.getName());
          customViewConfigurations.add(custom);
        }
      }

      customViewConfigurations.sort(Comparator.comparing(CustomViewConfiguration::getName));
    });

    super.finishDatabase();
  }

  private void mergeCustomViewConfiguration(String schemaName, String viewName) {
    CustomViewConfiguration merge = mergeConfiguration.getCustomViewConfiguration(schemaName, viewName);
    if (merge == null)
      return;

    LOGGER.info("Merging configuration of custom view {}.{}", schemaName, viewName);

    // Assume no copy on getters.
    // Allow setting description and columns, overriding query and primary key, and
    // adding foreign keys.
    CustomViewConfiguration view = moduleConfiguration.getCustomViewConfiguration(schemaName, viewName);
    if (merge.getDescription() != null) {
      view.setDescription(merge.getDescription());
    }
    if (!Constants.EMPTY.equals(merge.getQuery())) {
      view.setQuery(merge.getQuery());
    }
    if (!merge.getColumns().isEmpty()) {
      view.setColumns(merge.getColumns());
    }
    if (merge.getPrimaryKey() != null) {
      view.setPrimaryKey(merge.getPrimaryKey());
    }
    if (!merge.getForeignKeys().isEmpty()) {
      // Add, not replace!
      view.getForeignKeys().addAll(merge.getForeignKeys());
    }
  }

  private static String formatTblCol(String pattern, String tableName, String columnName) {
    return StringSubstitutor.replace(pattern, MapUtils.buildMapFromObjects("table", tableName, "column", columnName));
  }

  private String quoteSQL(String sqlName) {
    return noSQLQuotes ? sqlName : '"' + sqlName + '"';
  }

  private String getArrayNormalizationSQL(String schemaName, String tableName, String columnName,
    PrimaryKey primaryKey, List<CustomColumnConfiguration> columns) {

    // Resulting SQL is only tested in PostgreSQL, but "UNNEST ... WITH ORDINALITY" should be standard SQL.
    String indexColumnName = formatTblCol(arrayIndexColumnNamePattern, tableName, columnName);
    String itemColumnName = formatTblCol(arrayItemColumnNamePattern, tableName, columnName);
    String qColumnName = quoteSQL(columnName);
    String qIndexColumnName = quoteSQL(indexColumnName);
    String qItemColumnName = quoteSQL(itemColumnName);

    StringBuilder sb = getNormalizationSQLStringBuilder(ARRAY, tableName, primaryKey, columns).append(", ")
      .append(qIndexColumnName).append(", ").append(qItemColumnName).append(" ");
    addNormalizationSQLFrom(sb, schemaName, tableName) //
      .append(" cross join unnest(").append(qColumnName).append(") with ordinality as ") //
      .append(arrayTableAlias).append("(").append(qItemColumnName).append(", ").append(qIndexColumnName).append(")");

    addCustomColumnConfiguration(columns, indexColumnName, null,
      formatTblCol(arrayIndexColumnDescriptionPattern, tableName, columnName));
    // Item nullability defaults to false (assume no null items in array).
    addCustomColumnConfiguration(columns, itemColumnName, false,
      formatTblCol(arrayItemColumnDescriptionPattern, tableName, columnName));

    return sb.toString();
  }

  private String getJsonNormalizationSQL(String schemaName, String tableName, PrimaryKey primaryKey,
    List<CustomColumnConfiguration> columns) {
    // Get a template only; do not attempt to calculate columns which would require processing all rows in the table.
    StringBuilder sb = getNormalizationSQLStringBuilder(JSON, tableName, primaryKey, columns).append(" ");
    addNormalizationSQLFrom(sb, schemaName, tableName);

    return sb.toString();
  }

  private StringBuilder getNormalizationSQLStringBuilder(NormalizedColumnType ncType, String tableName,
    PrimaryKey primaryKey, List<CustomColumnConfiguration> columns) {

    StringBuilder sb = new StringBuilder("select");
    boolean first = true;

    for (String pkColumnName : primaryKey.getColumnNames()) {
      String name = formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
        tableName, pkColumnName);
      sb.append(first ? " " : ", ").append(pkColumnName).append(" as ").append(quoteSQL(name));
      addCustomColumnConfiguration(columns, name, null,
        formatTblCol(foreignIdColumnDescriptionPattern, tableName, pkColumnName));
      first = false;
    }

    return sb;
  }

  private StringBuilder addNormalizationSQLFrom(StringBuilder sb, String schemaName, String tableName) {
    String qSchemaName = quoteSQL(schemaName);
    String qTableName = quoteSQL(tableName);

    sb.append("from ").append(qSchemaName).append(".").append(qTableName);

    return sb;
  }

  private PrimaryKeyConfiguration getPrimaryKeyConfiguration(NormalizedColumnType ncType, PrimaryKey primaryKey,
    String tableName,
    String columnName) {
    PrimaryKeyConfiguration primaryKeyConfiguration = new PrimaryKeyConfiguration();
    List<String> columnNames = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      columnNames.add(formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
        tableName, pkColumnName));
    }

    if (ncType == ARRAY)
      columnNames.add(formatTblCol(arrayIndexColumnNamePattern, tableName, columnName));

    primaryKeyConfiguration.setColumnNames(columnNames);

    return primaryKeyConfiguration;
  }

  private ForeignKeyConfiguration getForeignKeyConfiguration(NormalizedColumnType ncType, PrimaryKey primaryKey,
    String tableName) {
    ForeignKeyConfiguration foreignKeyConfiguration = new ForeignKeyConfiguration();

    foreignKeyConfiguration.setReferencedTable(tableName);

    List<ReferenceConfiguration> refererences = new ArrayList<>(2);

    for (String pkColumnName : primaryKey.getColumnNames()) {
      ReferenceConfiguration ref = new ReferenceConfiguration();
      ref.setColumn(formatTblCol(ncType == ARRAY ? arrayForeignKeyColumnPattern : jsonForeignKeyColumnPattern,
        tableName, pkColumnName));
      ref.setReferenced(pkColumnName);
      refererences.add(ref);
    }
    foreignKeyConfiguration.setReferences(refererences);

    return foreignKeyConfiguration;
  }

  private void addCustomColumnConfiguration(List<CustomColumnConfiguration> list, String name, Boolean nillable,
    String description) {

    CustomColumnConfiguration customColumnConfiguration = new CustomColumnConfiguration();
    customColumnConfiguration.setName(name);
    customColumnConfiguration.setMerkle(false);
    customColumnConfiguration.setNillable(nillable);
    customColumnConfiguration.setDescription(description);

    list.add(customColumnConfiguration);
  }

  enum NormalizedColumnType {
    ARRAY, JSON
  }
}
