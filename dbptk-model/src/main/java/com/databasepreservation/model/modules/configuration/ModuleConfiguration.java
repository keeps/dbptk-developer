package com.databasepreservation.model.modules.configuration;

import static com.databasepreservation.Constants.CUSTOM_VIEW_NAME_PREFIX;
import static com.databasepreservation.Constants.VIEW_NAME_PREFIX;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.CANDIDATE_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.CHECK_CONSTRAINTS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.FOREIGN_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.PRIMARY_KEYS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.PRIVILEGES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.ROLES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.ROUTINES;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.TRIGGERS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.USERS;
import static com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata.VIEWS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata;
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
  private Map<DatabaseMetadata, Boolean> ignore;
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

  private boolean isIgnored(DatabaseMetadata databaseMetadata) {
    if (ignore.get(databaseMetadata) == null) {
      return false;
    }

    return ignore.get(databaseMetadata);
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
  public Map<DatabaseMetadata, Boolean> getIgnore() {
    return ignore;
  }

  public void setIgnore(Map<DatabaseMetadata, Boolean> ignore) {
    this.ignore = ignore;
  }

  public boolean isFetchRows() {
    return fetchRows;
  }

  public void setFetchRows(boolean fetchRows) {
    this.fetchRows = fetchRows;
  }
}
