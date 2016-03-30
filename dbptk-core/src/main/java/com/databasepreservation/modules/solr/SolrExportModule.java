package com.databasepreservation.modules.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

import com.databasepreservation.dbviewer.client.ViewerStructure.ViewerDatabase;
import com.databasepreservation.dbviewer.client.ViewerStructure.ViewerDatabaseFromToolkit;
import com.databasepreservation.dbviewer.transformers.ToolkitStructure2ViewerStructure;
import com.databasepreservation.dbviewer.utils.SolrManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.ModuleSettings;
import com.databasepreservation.model.structure.DatabaseStructure;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SolrExportModule implements DatabaseExportModule {
  private final SolrManager solrManager;

  private DatabaseStructure structure;

  private ViewerDatabaseFromToolkit viewerDatabase;

  public SolrExportModule() {
    String url = "http://127.0.0.1:8983/solr";
    solrManager = new SolrManager(url);
  }

  /**
   * Gets custom settings set by the export module that modify behaviour of the
   * import module.
   *
   * @throws ModuleException
   */
  @Override
  public ModuleSettings getModuleSettings() throws ModuleException {
    return null;
  }

  /**
   * Initialize the database, this will be the first method called
   *
   * @throws ModuleException
   */
  @Override
  public void initDatabase() throws ModuleException {

  }

  /**
   * Set ignored schemas. Ignored schemas won't be exported. This method should
   * be called before handleStructure. However, if not called it will be assumed
   * there are not ignored schemas.
   *
   * @param ignoredSchemas
   *          the set of schemas to ignored
   */
  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {

  }

  /**
   * Handle the database structure. This method will called after
   * setIgnoredSchemas.
   *
   * @param structure
   *          the database structure
   * @throws ModuleException
   * @throws UnknownTypeException
   */
  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException, UnknownTypeException {
    this.structure = structure;
    this.viewerDatabase = ToolkitStructure2ViewerStructure.getDatabase(structure);
    solrManager.addDatabase(viewerDatabase);
  }

  /**
   * Prepare to handle the data of a new schema. This method will be called
   * after handleStructure or handleDataCloseSchema.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    //viewerDatabase.getSchema(schemaName);
  }

  /**
   * Prepare to handle the data of a new table. This method will be called after
   * the handleDataOpenSchema, and before some calls to handleDataRow. If there
   * are no rows in the table, then handleDataCloseTable is called after this
   * method.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    solrManager.addTable(viewerDatabase.getTable(tableId));
  }

  /**
   * Handle a table row. This method will be called after the table was open and
   * before it was closed, by row index order.
   *
   * @param row
   *          the table row
   * @throws InvalidDataException
   * @throws ModuleException
   */
  @Override
  public void handleDataRow(Row row) throws InvalidDataException, ModuleException {

  }

  /**
   * Finish handling the data of a table. This method will be called after all
   * table rows for the table where requested to be handled.
   *
   * @param tableId
   *          the table id
   * @throws ModuleException
   */
  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {

  }

  /**
   * Finish handling the data of a schema. This method will be called after all
   * tables of the schema were requested to be handled.
   *
   * @param schemaName
   *          the schema name
   * @throws ModuleException
   */
  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {

  }

  /**
   * Finish the database. This method will be called when all data was requested
   * to be handled. This is the last method.
   *
   * @throws ModuleException
   */
  @Override
  public void finishDatabase() throws ModuleException {
    solrManager.commitAll();
    solrManager.freeResources();
  }
}
