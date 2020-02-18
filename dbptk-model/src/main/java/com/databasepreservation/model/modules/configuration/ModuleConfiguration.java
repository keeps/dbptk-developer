/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import static com.databasepreservation.Constants.VIEW_NAME_PREFIX;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.CANDIDATE_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.CHECK_CONSTRAINTS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.FOREIGN_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.PRIMARY_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.PRIVILEGES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.ROLES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.ROUTINES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.TRIGGERS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.USERS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.VIEWS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonPropertyOrder({"import", "schemas", "ignore"})
@JsonIgnoreProperties(value = {"fetchRows"})
public class ModuleConfiguration {

  private ImportModuleConfiguration importModuleConfiguration;
  private Map<String, SchemaConfiguration> schemaConfigurations;
  private Map<DatabaseTechnicalFeatures, Boolean> ignore;
  private boolean fetchRows;

  public ModuleConfiguration() {
    importModuleConfiguration = new ImportModuleConfiguration();
    schemaConfigurations = new LinkedHashMap<>();
    ignore = new LinkedHashMap<>();
    fetchRows = true;
  }

  /*
   * Behaviour Model
   */
  @JsonIgnore
  public boolean isSelectedSchema(String schemaName) {
    return schemaConfigurations.get(schemaName) != null || schemaConfigurations.isEmpty();
  }

  @JsonIgnore
  public boolean isSelectedTable(String schemaName, String tableName) {
    if (schemaConfigurations.isEmpty()) {
      return true;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).isSelectedTable(tableName);
  }

  @JsonIgnore
  public boolean isSelectedView(String schemaName, String viewName) {
    if (schemaConfigurations.isEmpty()) {
      return true;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).isSelectedView(viewName);
  }

  @JsonIgnore
  public boolean isSelectedColumnFromTable(String schemaName, String tableName, String columnName) {
    if (schemaConfigurations.isEmpty()) {
      return true;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).isSelectedColumnFromTable(tableName, columnName);
  }

  @JsonIgnore
  public boolean isSelectedColumnFromView(String schemaName, String viewName, String columnName) {
    if (schemaConfigurations.isEmpty()) {
      return true;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).isSelectedColumnFromView(viewName, columnName);
  }

  @JsonIgnore
  public boolean isMaterializeView(String schemaName, String viewName) {
    if (ignoreViews()) {
      return false;
    }

    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).isMaterializedView(viewName);

  }

  @JsonIgnore
  public List<CustomViewConfiguration> getCustomViews(String schemaName) {
    if (schemaConfigurations.get(schemaName) == null) {
      return new ArrayList<>();
    }

    return schemaConfigurations.get(schemaName).getCustomViewConfigurations();
  }

  @JsonIgnore
  public TableConfiguration getTableConfiguration(String schemaName, String name) {
    if (schemaConfigurations.get(schemaName) == null) {
      return null;
    }

    return schemaConfigurations.get(schemaName).getTableConfiguration(name);
  }

  @JsonIgnore
  public CustomViewConfiguration getCustomViewConfiguration(String schemaName, String name) {
    if (schemaConfigurations.get(schemaName) == null) {
      return null;
    }

    for (CustomViewConfiguration customViewConfiguration : schemaConfigurations.get(schemaName)
      .getCustomViewConfigurations()) {
      if (customViewConfiguration.getName().equals(name)) {
        return customViewConfiguration;
      }
    }

    return null;
  }

  @JsonIgnore
  public boolean ignoreRoutines() {
    return isIgnored(ROUTINES);
  }

  @JsonIgnore
  public boolean ignoreTriggers() {
    return isIgnored(TRIGGERS);
  }

  @JsonIgnore
  public boolean ignoreUsers() {
    return isIgnored(USERS);
  }

  @JsonIgnore
  public boolean ignoreRoles() {
    return isIgnored(ROLES);
  }

  @JsonIgnore
  public boolean ignorePrivileges() {
    return isIgnored(PRIVILEGES);
  }

  @JsonIgnore
  public boolean ignorePrimaryKey() {
    return isIgnored(PRIMARY_KEYS);
  }

  @JsonIgnore
  public boolean ignoreCandidateKey() {
    return isIgnored(CANDIDATE_KEYS);
  }

  @JsonIgnore
  public boolean ignoreCheckConstraints() {
    return isIgnored(CHECK_CONSTRAINTS);
  }

  @JsonIgnore
  public boolean ignoreForeignKey() {
    return isIgnored(FOREIGN_KEYS);
  }

  @JsonIgnore
  public boolean ignoreViews() {
    return isIgnored(VIEWS);
  }

  private boolean isIgnored(DatabaseTechnicalFeatures databaseTechnicalFeatures) {
    if (ignore.get(databaseTechnicalFeatures) == null) {
      return false;
    }

    return ignore.get(databaseTechnicalFeatures);
  }

  @JsonIgnore
  public String getImportModuleParameterValue(String parameter) {
    return importModuleConfiguration.getParameters().get(parameter);
  }

  @JsonIgnore
  private boolean isWhereDefinedForTable(String schemaName, String tableName) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).getTableConfiguration(tableName) != null
      && !schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getWhere().equals(Constants.EMPTY);
  }

  @JsonIgnore
  private boolean isWhereDefinedForView(String schemaName, String viewName) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).getViewConfiguration(viewName) != null
      && !schemaConfigurations.get(schemaName).getTableConfiguration(viewName).getWhere().equals(Constants.EMPTY);
  }

  @JsonIgnore
  public String getWhere(String schemaName, String tableName, boolean isTable) {
    if (isTable) {
      if (isWhereDefinedForTable(schemaName, tableName)) {
        return schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getWhere();
      }
    } else {
      if (isWhereDefinedForView(schemaName, tableName)) {
        return schemaConfigurations.get(schemaName).getViewConfiguration(tableName).getWhere();
      }
    }

    return null;
  }

  @JsonIgnore
  private boolean isOrderByDefinedForTable(String schemaName, String tableName) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).getTableConfiguration(tableName) != null
      && !schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getOrderBy().equals(Constants.EMPTY);
  }

  @JsonIgnore
  private boolean isOrderByDefinedForView(String schemaName, String viewName) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).getViewConfiguration(viewName) != null
      && !schemaConfigurations.get(schemaName).getViewConfiguration(viewName).getOrderBy().equals(Constants.EMPTY);
  }

  public String getOrderBy(String schemaName, String tableName, boolean isTable) {
    if (isTable) {
      if (isOrderByDefinedForTable(schemaName, tableName)) {
        return schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getOrderBy();
      }
    } else {
      String viewNameWithoutPrefix = tableName.replace(VIEW_NAME_PREFIX, "");
      if (isOrderByDefinedForView(schemaName, viewNameWithoutPrefix)) {
        return schemaConfigurations.get(schemaName).getViewConfiguration(viewNameWithoutPrefix).getOrderBy();
      }
    }

    return null;
  }

  @JsonIgnore
  public boolean hasExternalLobDefined(String schemaName, String tableName) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    if (schemaConfigurations.get(schemaName) != null
      && schemaConfigurations.get(schemaName).getTableConfiguration(tableName) != null) {
      return schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getColumns().stream()
        .anyMatch(p -> p.getExternalLob() != null);
    }

    return false;
  }

  @JsonIgnore
  public boolean isExternalLobColumn(String schema, String table, String column) {
    if (schemaConfigurations.isEmpty()) {
      return false;
    }

    return schemaConfigurations.get(schema) != null
      && schemaConfigurations.get(schema).getTableConfiguration(table) != null
      && schemaConfigurations.get(schema).getTableConfiguration(table).getColumnConfiguration(column) != null
      && schemaConfigurations.get(schema).getTableConfiguration(table).getColumnConfiguration(column)
        .getExternalLob() != null;
  }

  @JsonIgnore
  public ExternalLobsConfiguration getExternalLobsConfiguration(String schemaName, String tableName,
    String columnName) {
    if (isExternalLobColumn(schemaName, tableName, columnName)) {
      return schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getColumnConfiguration(columnName)
        .getExternalLob();
    }

    return null;
  }

  @JsonIgnore
  public List<ExternalLobsConfiguration> getExternalLobsConfigurations(String schemaName, String tableName) {
    List<ExternalLobsConfiguration> externalLobsConfigurations = new ArrayList<>();
    if (hasExternalLobDefined(schemaName, tableName)) {
      externalLobsConfigurations = schemaConfigurations.get(schemaName).getTableConfiguration(tableName).getColumns()
        .stream().map(ColumnConfiguration::getExternalLob).collect(Collectors.toList());
    }

    return externalLobsConfigurations;
  }

  @JsonProperty("import")
  public ImportModuleConfiguration getImportModuleConfiguration() {
    return importModuleConfiguration;
  }

  public void setImportModuleConfiguration(ImportModuleConfiguration importModuleConfiguration) {
    this.importModuleConfiguration = importModuleConfiguration;
  }

  @JsonProperty("schemas")
  public Map<String, SchemaConfiguration> getSchemaConfigurations() {
    return schemaConfigurations;
  }

  public void setSchemaConfigurations(Map<String, SchemaConfiguration> schemaConfigurations) {
    this.schemaConfigurations = schemaConfigurations;
  }

  @JsonProperty("ignore")
  public Map<DatabaseTechnicalFeatures, Boolean> getIgnore() {
    return ignore;
  }

  public void setIgnore(Map<DatabaseTechnicalFeatures, Boolean> ignore) {
    this.ignore = ignore;
  }

  public boolean isFetchRows() {
    return fetchRows;
  }

  public void setFetchRows(boolean fetchRows) {
    this.fetchRows = fetchRows;
  }
}
