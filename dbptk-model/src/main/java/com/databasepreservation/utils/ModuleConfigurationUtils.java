/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import static com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures.*;

import java.util.*;

import com.databasepreservation.model.modules.configuration.*;
import com.databasepreservation.model.modules.configuration.enums.DatabaseTechnicalFeatures;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ModuleConfigurationUtils {

  /**
   * Initialize a <code>ModuleConfiguration</code> object it default configuration
   * options
   *
   * @return A <code>ModuleConfiguration</code>
   */
  public static ModuleConfiguration getDefaultModuleConfiguration() {
    ModuleConfiguration moduleConfiguration = new ModuleConfiguration();
    moduleConfiguration.setIgnore(createDefaultIgnoreList(false));

    return moduleConfiguration;
  }

  public static void addCustomViewConfiguration(ModuleConfiguration moduleConfiguration, String schemaName, String name,
    boolean simulateTable, String description, String query, List<CustomColumnConfiguration> columns,
    PrimaryKeyConfiguration primaryKey, List<ForeignKeyConfiguration> foreignKeys) {

    SchemaConfiguration schemaConfiguration = moduleConfiguration.getSchemaConfigurations().get(schemaName);
    if (schemaConfiguration == null) {
      schemaConfiguration = new SchemaConfiguration();
    }

    CustomViewConfiguration customViewConfiguration = new CustomViewConfiguration();
    customViewConfiguration.setName(name);
    customViewConfiguration.setSimulateTable(simulateTable);
    customViewConfiguration.setDescription(description);
    customViewConfiguration.setQuery(query);
    customViewConfiguration.setColumns(columns);
    customViewConfiguration.setPrimaryKey(primaryKey);
    customViewConfiguration.setForeignKeys(new ArrayList<>(foreignKeys)); // Ensure that items can be added.

    schemaConfiguration.getCustomViewConfigurations().add(customViewConfiguration);
    moduleConfiguration.getSchemaConfigurations().put(schemaName, schemaConfiguration);
  }

  public static void addTableConfiguration(ModuleConfiguration moduleConfiguration, TableStructure table) {
    SchemaConfiguration schemaConfiguration = moduleConfiguration.getSchemaConfigurations().get(table.getSchema());
    if (schemaConfiguration == null) {
      schemaConfiguration = new SchemaConfiguration();
    }
    schemaConfiguration.getTableConfigurations().add(getTableConfiguration(table));
    moduleConfiguration.getSchemaConfigurations().put(table.getSchema(), schemaConfiguration);
  }

  public static void addViewConfiguration(ModuleConfiguration moduleConfiguration, ViewStructure view, String schema) {
    SchemaConfiguration schemaConfiguration = moduleConfiguration.getSchemaConfigurations().get(schema);
    if (schemaConfiguration == null) {
      schemaConfiguration = new SchemaConfiguration();
    }
    schemaConfiguration.getViewConfigurations().add(getViewConfiguration(view));
    moduleConfiguration.getSchemaConfigurations().put(schema, schemaConfiguration);
  }

  private static TableConfiguration getTableConfiguration(TableStructure table) {
    TableConfiguration tableConfiguration = new TableConfiguration();
    tableConfiguration.setName(table.getName());
    table.getColumns().forEach(column -> tableConfiguration.getColumns().add(getColumnConfiguration(column)));

    return tableConfiguration;
  }

  private static ColumnConfiguration getColumnConfiguration(ColumnStructure column) {
    ColumnConfiguration columnConfiguration = new ColumnConfiguration();
    columnConfiguration.setName(column.getName());
    columnConfiguration.setMerkle(false);

    return columnConfiguration;
  }

  private static ViewConfiguration getViewConfiguration(ViewStructure view) {
    ViewConfiguration viewConfiguration = new ViewConfiguration();
    viewConfiguration.setName(view.getName());
    viewConfiguration.setMaterialized(false);
    view.getColumns().forEach(column -> viewConfiguration.getColumns().add(getColumnConfiguration(column)));

    return viewConfiguration;
  }

  public static Map<DatabaseTechnicalFeatures, Boolean> createIgnoreListExcept(boolean value, DatabaseTechnicalFeatures... whitelist) {
    final Map<DatabaseTechnicalFeatures, Boolean> ignoreList = createIgnoreList(value);
    List<DatabaseTechnicalFeatures> databaseTechnicalFeaturesList = Arrays.asList(whitelist);

    databaseTechnicalFeaturesList.forEach(m -> {
      ignoreList.put(m, !value);
    });

    return ignoreList;
  }

  public static Map<DatabaseTechnicalFeatures, Boolean> createIgnoreList(boolean value) {
    return createDefaultIgnoreList(value);
  }

  private static Map<DatabaseTechnicalFeatures, Boolean> createDefaultIgnoreList(boolean value) {
    Map<DatabaseTechnicalFeatures, Boolean> ignores = new LinkedHashMap<>();

    ignores.put(USERS, value);
    ignores.put(ROLES, value);
    ignores.put(PRIVILEGES, value);
    ignores.put(ROUTINES, value);
    ignores.put(TRIGGERS, value);
    ignores.put(PRIMARY_KEYS, value);
    ignores.put(CANDIDATE_KEYS, value);
    ignores.put(FOREIGN_KEYS, value);
    ignores.put(CHECK_CONSTRAINTS, value);
    ignores.put(VIEWS, value);

    return ignores;
  }

  public static void addImportParameters(ModuleConfiguration moduleConfiguration, String moduleName,
                                         Map<String, String> properties, Map<String, String> remoteProperties) {
    ImportModuleConfiguration importModuleConfiguration = new ImportModuleConfiguration();
    importModuleConfiguration.setModuleName(moduleName);

    if (remoteProperties != null && !remoteProperties.isEmpty()) {
      properties.putAll(remoteProperties);
    }

    importModuleConfiguration.setParameters(properties);
    moduleConfiguration.setImportModuleConfiguration(importModuleConfiguration);
  }
}
