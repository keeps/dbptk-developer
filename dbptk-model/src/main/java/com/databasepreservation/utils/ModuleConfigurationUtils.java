package com.databasepreservation.utils;

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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.databasepreservation.Constants;
import com.databasepreservation.model.modules.configuration.ColumnConfiguration;
import com.databasepreservation.model.modules.configuration.CustomViewConfiguration;
import com.databasepreservation.model.modules.configuration.ImportModuleConfiguration;
import com.databasepreservation.model.modules.configuration.ModuleConfiguration;
import com.databasepreservation.model.modules.configuration.SchemaConfiguration;
import com.databasepreservation.model.modules.configuration.TableConfiguration;
import com.databasepreservation.model.modules.configuration.ViewConfiguration;
import com.databasepreservation.model.modules.configuration.enums.DatabaseMetadata;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;

import javax.xml.crypto.Data;

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
                                                String description, String query) {
    SchemaConfiguration schemaConfiguration = moduleConfiguration.getSchemaConfigurations().get(schemaName);
    if (schemaConfiguration == null) {
      schemaConfiguration = new SchemaConfiguration();
    }

    CustomViewConfiguration customViewConfiguration = new CustomViewConfiguration();
    customViewConfiguration.setName(name);
    customViewConfiguration.setDescription(description);
    customViewConfiguration.setQuery(query);

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

    return columnConfiguration;
  }

  private static ViewConfiguration getViewConfiguration(ViewStructure view) {
    ViewConfiguration viewConfiguration = new ViewConfiguration();
    viewConfiguration.setName(view.getName());
    viewConfiguration.setMaterialized(false);
    view.getColumns().forEach(column -> viewConfiguration.getColumns().add(column.getName()));

    return viewConfiguration;
  }

  public static Map<DatabaseMetadata, Boolean> createIgnoreListExcept(boolean value, DatabaseMetadata... whitelist) {
    final Map<DatabaseMetadata, Boolean> ignoreList = createIgnoreList(value);
    List<DatabaseMetadata> databaseMetadataList = Arrays.asList(whitelist);

    databaseMetadataList.forEach(m -> {
      ignoreList.put(m, !value);
    });

    return ignoreList;
  }

  public static Map<DatabaseMetadata, Boolean> createIgnoreList(boolean value) {
    return createDefaultIgnoreList(value);
  }

  private static Map<DatabaseMetadata, Boolean> createDefaultIgnoreList(boolean value) {
    Map<DatabaseMetadata, Boolean> ignores = new LinkedHashMap<>();

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
      properties.put(Constants.DB_SSH, "true");
      properties.putAll(remoteProperties);
    }

    importModuleConfiguration.setParameters(properties);
    moduleConfiguration.setImportModuleConfiguration(importModuleConfiguration);
  }
}
