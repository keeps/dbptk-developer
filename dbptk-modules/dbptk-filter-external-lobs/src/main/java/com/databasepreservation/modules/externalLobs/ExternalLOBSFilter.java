package com.databasepreservation.modules.externalLobs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.managers.ModuleConfigurationManager;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.configuration.ExternalLobsConfiguration;
import com.databasepreservation.model.modules.configuration.FileExternalLobsConfiguration;
import com.databasepreservation.model.modules.configuration.RemoteExternalLobsConfiguration;
import com.databasepreservation.model.modules.configuration.S3AWSExternalLobsConfiguration;
import com.databasepreservation.model.modules.configuration.S3MinIOExternalLobsConfiguration;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerFileSystem;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerRemoteFileSystem;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerS3AWS;
import com.databasepreservation.modules.externalLobs.CellHandlers.ExternalLOBSCellHandlerS3MinIO;

public class ExternalLOBSFilter implements DatabaseFilterModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSFilter.class);
  private static final Integer MAX_QUEUE_SIZE = 100;

  private DatabaseFilterModule exportModule;
  private Reporter reporter;
  private Map<String, ExternalLobsConfiguration> externalLobsConfigurations = new HashMap<>();
  private DatabaseStructure databaseStructure;
  private TableStructure currentTable = null;
  private boolean hasExternalLOBS = false;
  private List<Integer> externalLOBIndexes = new ArrayList<>();

  // Async Pipeline Attributes
  private ExecutorService executorService;
  private ExecutorService writerExecutor;
  private BlockingQueue<Future<Row>> pendingRowsQueue;
  private Future<?> writerTask;
  private final AtomicReference<Throwable> writerError = new AtomicReference<>();

  public ExternalLOBSFilter() {
    // Empty constructor
  }

  @Override
  public DatabaseFilterModule migrateDatabaseTo(DatabaseFilterModule exportModule) throws ModuleException {
    this.exportModule = exportModule;
    return this;
  }

  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public void initDatabase() throws ModuleException {
    // Initialize thread pools. Virtual threads are excellent for I/O bound wait times.
    this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    this.writerExecutor = Executors.newSingleThreadExecutor();
    this.exportModule.initDatabase();
  }

  @Override
  public void setIgnoredSchemas(Set<String> ignoredSchemas) {
    this.exportModule.setIgnoredSchemas(ignoredSchemas);
  }

  @Override
  public void handleStructure(DatabaseStructure structure) throws ModuleException {
    for (SchemaStructure schema : structure.getSchemas()) {
      for (TableStructure table : schema.getTables()) {
        for (ColumnStructure column : table.getColumns()) {
          if (ModuleConfigurationManager.getInstance().getModuleConfiguration().isExternalLobColumn(schema.getName(),
                  table.getName(), column.getName(), table.isFromView(), table.isFromCustomView())) {
            StringBuilder description = new StringBuilder("Converted to LOB referenced by");
            final ExternalLobsConfiguration externalLobsConfiguration = ModuleConfigurationManager.getInstance()
                    .getModuleConfiguration().getExternalLobsConfiguration(schema.getName(), table.getName(),
                            column.getName(), table.isFromView(), table.isFromCustomView());

            description.append(getTypeOfExternalLobsConfiguration(externalLobsConfiguration));

            Type original = column.getType();
            description.append(". Original description: '").append(original.getDescription()).append("')");
            SimpleTypeBinary newType = new SimpleTypeBinary();
            newType.setSql99TypeName("BINARY VARYING", 1);
            newType.setSql2008TypeName("BINARY VARYING", 1);
            newType.setOriginalTypeName(original.getOriginalTypeName());
            newType.setOutsideDatabase(true);

            column.setType(newType);
            column.setDescription(description.toString());
          }
        }
      }
    }

    this.databaseStructure = structure;
    this.exportModule.handleStructure(structure);
  }

  @Override
  public void handleDataOpenSchema(String schemaName) throws ModuleException {
    this.exportModule.handleDataOpenSchema(schemaName);
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    currentTable = databaseStructure.getTableById(tableId);
    final boolean hasExternalLobDefined = ModuleConfigurationManager.getInstance().getModuleConfiguration()
            .hasExternalLobDefined(currentTable.getSchema(), currentTable.getName(), currentTable.isFromView(),
                    currentTable.isFromCustomView());

    if (hasExternalLobDefined) {
      hasExternalLOBS = true;
      final List<ColumnStructure> columns = currentTable.getColumns();

      for (int i = 0; i < columns.size(); i++) {
        if (ModuleConfigurationManager.getInstance().getModuleConfiguration().isExternalLobColumn(
                currentTable.getSchema(), currentTable.getName(), columns.get(i).getName(), currentTable.isFromView(),
                currentTable.isFromCustomView())) {
          externalLOBIndexes.add(i);
          externalLobsConfigurations.put(tableId + i,
                  ModuleConfigurationManager.getInstance().getModuleConfiguration().getExternalLobsConfiguration(
                          currentTable.getSchema(), currentTable.getName(), columns.get(i).getName(), currentTable.isFromView(),
                          currentTable.isFromCustomView()));
        }
      }

      // Initialize the async processing pipeline for this table
      this.pendingRowsQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
      this.writerError.set(null);
      startAsyncWriter();
    }

    this.exportModule.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    if (hasExternalLOBS) {
      enqueueRow(row);
    } else {
      this.exportModule.handleDataRow(row);
    }
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    if (hasExternalLOBS) {
      stopAsyncWriter(); // Drain pipeline sequentially before closing the table
    }

    hasExternalLOBS = false;
    externalLOBIndexes = new ArrayList<>();
    currentTable = null;
    externalLobsConfigurations = new HashMap<>();
    this.exportModule.handleDataCloseTable(tableId);
  }

  @Override
  public void handleDataCloseSchema(String schemaName) throws ModuleException {
    this.exportModule.handleDataCloseSchema(schemaName);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
    }
    if (writerExecutor != null && !writerExecutor.isShutdown()) {
      writerExecutor.shutdown();
    }
    this.exportModule.finishDatabase();
  }

  @Override
  public void updateModuleConfiguration(String moduleName, Map<String, String> properties,
                                        Map<String, String> remoteProperties) {
    // do nothing
  }

  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return null;
  }

  // --- ASYNC PIPELINE METHODS ---

  private void startAsyncWriter() {
    this.writerTask = writerExecutor.submit(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Future<Row> future = pendingRowsQueue.take();
          Row processedRow = future.get(); // Awaits download boundary

          if (processedRow == null) {
            break; // Poison Pill received, exit loop cleanly
          }

          exportModule.handleDataRow(processedRow);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOGGER.error("Background LOB task failed: {}", e.getCause().getMessage(), e);
        writerError.set(e.getCause());
      } catch (Exception e) {
        LOGGER.error("Unexpected pipeline failure.", e);
        writerError.set(e);
      }
    });
  }

  private void enqueueRow(Row row) throws ModuleException {
    if (writerError.get() != null) {
      throw new ModuleException().withMessage("Aborted due to previous background failure")
              .withCause(writerError.get());
    }

    Callable<Row> conversionTask = () -> processRowAsync(row);
    Future<Row> future = executorService.submit(conversionTask);

    try {
      while (!pendingRowsQueue.offer(future, 500, TimeUnit.MILLISECONDS)) {
        if (writerError.get() != null) {
          future.cancel(true);
          throw new ModuleException().withMessage("Halted while enqueuing row").withCause(writerError.get());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.cancel(true);
      throw new ModuleException().withMessage("Row enqueue interrupted").withCause(e);
    }
  }

  private Row processRowAsync(Row row) throws ModuleException {
    List<Cell> rowCells = row.getCells();

    for (int index : externalLOBIndexes) {
      Cell cell = rowCells.get(index);

      if (cell instanceof SimpleCell simpleCell) {
        String reference = simpleCell.getSimpleData();
        if (reference != null && !reference.isEmpty()) {
          final ExternalLobsConfiguration config = externalLobsConfigurations.get(currentTable.getId() + index);
          Cell newCell = getExternalLOBSCellHandler(config).handleCell(cell.getId(), simpleCell.getSimpleData());
          rowCells.set(index, newCell);
        } else {
          reporter.ignored("Cell " + cell.getId(), "reference to external LOB is null");
          rowCells.set(index, new NullCell(cell.getId()));
        }
      } else {
        LOGGER.error("Reference to LOB is not a SimpleCell");
        rowCells.set(index, new NullCell(cell.getId()));
      }
    }

    row.setCells(rowCells);
    return row;
  }

  private void stopAsyncWriter() throws ModuleException {
    if (writerError.get() == null) {
      try {
        Future<Row> poisonPill = executorService.submit(() -> null);
        while (!pendingRowsQueue.offer(poisonPill, 500, TimeUnit.MILLISECONDS)) {
          if (writerError.get() != null) break;
        }

        if (writerTask != null) {
          writerTask.get(); // Wait for the consumer to finish cleanly
        }
      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        throw new ModuleException().withMessage("Failed to shut down background writer").withCause(e);
      }
    }

    // Purge queue if aborting due to error
    if (pendingRowsQueue != null && !pendingRowsQueue.isEmpty()) {
      for (Future<Row> future : pendingRowsQueue) {
        future.cancel(true);
      }
      pendingRowsQueue.clear();
    }

    if (writerError.get() != null) {
      throw new ModuleException().withMessage("Pipeline failed").withCause(writerError.get());
    }
  }

  // --- FACTORY METHODS ---

  private ExternalLOBSCellHandler getExternalLOBSCellHandler(ExternalLobsConfiguration configuration)
          throws ModuleException {

    if (configuration instanceof FileExternalLobsConfiguration fileConfiguration) {
      return new ExternalLOBSCellHandlerFileSystem(Paths.get(fileConfiguration.getBasePath()), reporter);
    }

    if (configuration instanceof RemoteExternalLobsConfiguration remoteExternalLobsConfiguration) {
      return new ExternalLOBSCellHandlerRemoteFileSystem(Paths.get(remoteExternalLobsConfiguration.getBasePath()),
              reporter);
    }

    if (configuration instanceof S3AWSExternalLobsConfiguration s3AWSExternalLobsConfiguration) {
      return new ExternalLOBSCellHandlerS3AWS(s3AWSExternalLobsConfiguration.getEndpoint(),
              s3AWSExternalLobsConfiguration.getRegion(), s3AWSExternalLobsConfiguration.getBucketName(),
              s3AWSExternalLobsConfiguration.getAccessKey(), s3AWSExternalLobsConfiguration.getSecretKey(), reporter);
    }

    if (configuration instanceof S3MinIOExternalLobsConfiguration s3MinIOExternalLobsConfiguration) {
      return new ExternalLOBSCellHandlerS3MinIO(s3MinIOExternalLobsConfiguration.getEndpoint(),
              s3MinIOExternalLobsConfiguration.getBucketName(), s3MinIOExternalLobsConfiguration.getAccessKey(),
              s3MinIOExternalLobsConfiguration.getSecretKey(), reporter);
    }

    throw new ModuleException().withMessage("Unrecognized reference type");
  }

  private String getTypeOfExternalLobsConfiguration(ExternalLobsConfiguration configuration) throws ModuleException {
    if (configuration instanceof FileExternalLobsConfiguration) {
      return " file system path";
    } else if (configuration instanceof RemoteExternalLobsConfiguration) {
      return " remove file system path";
    } else if (configuration instanceof S3AWSExternalLobsConfiguration) {
      return " S3 AWS";
    } else if (configuration instanceof S3MinIOExternalLobsConfiguration) {
      return " S3 MinIO";
    }

    throw new ModuleException().withMessage("Unrecognized reference type");
  }
}