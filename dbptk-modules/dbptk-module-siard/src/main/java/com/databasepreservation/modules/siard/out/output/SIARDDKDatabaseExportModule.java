/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.output;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.PathInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.SIARDDKModuleFactory;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.services.conversion.HttpLobConversionService;
import com.databasepreservation.modules.siard.services.conversion.HttpLobConversionServiceException;
import com.databasepreservation.modules.siard.services.conversion.LobConversionService;
import com.databasepreservation.modules.siard.services.conversion.TempFileTracker;
import com.databasepreservation.modules.siard.services.conversion.model.ConversionResult;
import com.databasepreservation.utils.ConfigUtils;

/**
 * Handles database export pipeline matching SIARD-DK compliance, incorporating
 * high-throughput asynchronous LOB streaming conversion.
 * 
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKDatabaseExportModule extends SIARDExportDefault {

  private final SIARDDKExportModule siarddkExportModule;
  private static final Logger logger = LoggerFactory.getLogger(SIARDDKDatabaseExportModule.class);

  private ExecutorService executorService;
  private TempFileTracker tempFileTracker;
  private LobConversionService conversionService;
  private String targetLobFormat;
  private static final Integer MAX_QUEUE_SIZE = ConfigUtils.getProperty(100, "dbptk.siarddk.export.maxQueueSize");

  // Resilient Pipeline architecture attributes
  private BlockingQueue<Future<ProcessedRowContext>> pendingRowsQueue;
  private ExecutorService writerExecutor;
  private Future<?> writerTask;
  private final AtomicReference<Throwable> writerError = new AtomicReference<>();

  /**
   * Data context tuple to link rows with their transient extracted disk paths.
   */
  private record ProcessedRowContext(Row row, List<Path> transientPaths) {
  }

  public SIARDDKDatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    super(siarddkExportModule.getContentExportStrategy(), siarddkExportModule.getMainContainer(),
      siarddkExportModule.getWriteStrategy(), siarddkExportModule.getMetadataExportStrategy(), null);

    this.siarddkExportModule = siarddkExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    Map<String, String> exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    boolean isLobConversionEnabled = Boolean
      .parseBoolean(exportModuleArgs.getOrDefault(SIARDDKModuleFactory.PARAMETER_LOB_CONVERSION_ENABLED, "false"));

    if (isLobConversionEnabled) {
      this.tempFileTracker = new TempFileTracker();
      String apiEndpoint = exportModuleArgs.getOrDefault(SIARDDKModuleFactory.PARAMETER_LOB_CONVERSION_ENDPOINT,
        "http://localhost:8087");
      this.targetLobFormat = exportModuleArgs.getOrDefault(SIARDDKModuleFactory.PARAMETER_LOB_CONVERSION_TARGET_FORMAT,
        "image/tiff");

      this.conversionService = new HttpLobConversionService(apiEndpoint, this.targetLobFormat, this.tempFileTracker);
      logger.info("LOB conversion service enabled. Endpoint: '{}', Target Format: '{}'", apiEndpoint,
        this.targetLobFormat);
    } else {
      this.conversionService = null;
      this.tempFileTracker = null;
      logger.info("LOB conversion service is disabled.");
    }

    this.executorService = Executors.newVirtualThreadPerTaskExecutor();
    this.writerExecutor = Executors.newSingleThreadExecutor(); // Dedicated single-thread pipeline consumer

    // Get docID info from the command line and add these to the LOBsTracker

    Path pathToArchive = siarddkExportModule.getMainContainer().getPath();

    // Check if the archive folder name is correct (must match
    // AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*)

    String regex = "AVID.[A-ZÆØÅ]{2,4}.[1-9][0-9]*.[1-9][0-9]*";
    String folderName = pathToArchive.getFileName().toString();
    if (!folderName.matches(regex)) {
      throw new ModuleException().withMessage("Archive folder name must match the expression " + regex);
    }

    // Backup output folder if it already exists

    File outputFolder = pathToArchive.toFile();

    if (outputFolder.isDirectory()) {
      try {

        // Get the creation time of the old archive folder
        BasicFileAttributes basicFileAttributes = Files.readAttributes(pathToArchive, BasicFileAttributes.class);
        String creationTimeStamp = basicFileAttributes.creationTime().toString();

        String name = pathToArchive.toString() + "_backup_" + creationTimeStamp;

        // Rename the old folder
        File oldArchiveDir = new File(name.replaceAll("[:\\\\/*?|<>]", "_"));
        FileUtils.moveDirectory(outputFolder, oldArchiveDir);

        logger.info("Backed up an already existing archive folder to: " + oldArchiveDir);
      } catch (IOException e) {
        throw new ModuleException().withMessage("Error deleting existing directory").withCause(e);
      }
    }
  }

  @Override
  public void handleDataOpenTable(String tableId) throws ModuleException {
    logger.debug("Opening table '{}'. Initializing asynchronous pipeline...", tableId);
    this.pendingRowsQueue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    this.writerError.set(null);
    startAsyncWriter();
    super.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    enqueueRow(row);
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    logger.debug("Closing table '{}'. Draining remaining items in the pipeline...", tableId);
    stopAsyncWriter();
    super.handleDataCloseTable(tableId);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
    }
    if (writerExecutor != null && !writerExecutor.isShutdown()) {
      writerExecutor.shutdown();
    }
    if (tempFileTracker != null) {
      tempFileTracker.cleanupAll();
    }

    super.finishDatabase();
    // Write ContextDocumentation to archive

    Map<String, String> exportModuleArgs = siarddkExportModule.getExportModuleArgs();
    SIARDDKFileIndexFileStrategy SIARDDKFileIndexFileStrategy = siarddkExportModule.getFileIndexFileStrategy();
    MetadataPathStrategy metadataPathStrategy = siarddkExportModule.getMetadataPathStrategy();
    SIARDMarshaller siardMarshaller = siarddkExportModule.getSiardMarshaller();

    if (exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER) != null) {

      SIARDDKContextDocumentationWriter SIARDDKContextDocumentationWriter = new SIARDDKContextDocumentationWriter(
        siarddkExportModule.getMainContainer(), siarddkExportModule.getWriteStrategy(), SIARDDKFileIndexFileStrategy,
        siarddkExportModule.getExportModuleArgs());

      SIARDDKContextDocumentationWriter.writeContextDocumentation();
    }

    // Create fileIndex.xml

    // TO-DO: refactor the stuff below into separate class (also to be used by
    // the MetadataExportStrategy)

    try {
      SIARDDKFileIndexFileStrategy.generateXML(null);
    } catch (ModuleException e) {
      throw new ModuleException().withMessage("Error writing fileIndex.xml").withCause(e);
    }

    try {
      String path = metadataPathStrategy.getXmlFilePath(SIARDDKConstants.FILE_INDEX);
      OutputStream writer = SIARDDKFileIndexFileStrategy.getWriter(siarddkExportModule.getMainContainer(), path,
        siarddkExportModule.getWriteStrategy());

      // Making the SIARD DK XML namespace default in case needed
      Map<String, String> namespaceMap = new HashMap<>();
      namespaceMap.put("http://www.sa.dk/xmlns/diark/1.0", "");

      siardMarshaller.marshal(getJAXBContextClass(),
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        SIARDDKFileIndexFileStrategy.generateXML(null), namespaceMap);

      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing fileIndex to the archive.").withCause(e);
    }
  }

  /**
   * Starts the sequential pipeline background consumer thread.
   */
  private void startAsyncWriter() {
    this.writerTask = writerExecutor.submit(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          logger.debug("Consumer thread is waiting to take the oldest row from the queue...");
          Future<ProcessedRowContext> future = pendingRowsQueue.take(); // Enforces strict sequential order

          logger.debug("Oldest row taken. Awaiting its Virtual Thread completion (LOB HTTP boundary)...");
          ProcessedRowContext context = future.get(); // Awaits specific LOB HTTP processing boundary

          if (context == null) {
            logger.debug("<< DEQUEUED: Poison Pill received. Safely shutting down the consumer thread.");
            break;
          }

          super.handleDataRow(context.row());

          // Alleviate disk pressure by wiping extracted structures instantly after XML
          // writing
          cleanupTransientPaths(context.transientPaths());
          logger.debug("<< DEQUEUED: Row [{}] removed from queue and successfully written. Current size: {}/{}",
            context.row().getIndex(), pendingRowsQueue.size(), MAX_QUEUE_SIZE);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.debug("Pipeline consumer thread was interrupted and is shutting down.");
      } catch (ExecutionException e) {
        logger.error("A background conversion task failed critically: {}", e.getCause().getMessage());
        writerError.set(e.getCause());
      } catch (Exception e) {
        logger.error("An unexpected error occurred during sequential writing.", e);
        writerError.set(e);
      }
    });
  }

  /**
   * Gracefully drains the remaining queue, safely stops the consumer and monitors
   * failures.
   */
  private void stopAsyncWriter() throws ModuleException {
    if (writerError.get() == null) {
      try {
        logger.debug("|| TABLE END: Injecting Poison Pill into the queue and waiting for consumer to finish...");

        Future<ProcessedRowContext> poisonPill = executorService.submit(() -> null);

        while (!pendingRowsQueue.offer(poisonPill, 500, TimeUnit.MILLISECONDS)) {
          if (writerError.get() != null)
            break;
        }

        if (writerTask != null) {
          writerTask.get();
        }

      } catch (InterruptedException | ExecutionException e) {
        Thread.currentThread().interrupt();
        throw new ModuleException().withMessage("Failed to cleanly stop the background writer").withCause(e);
      }
    }

    // Purge any remaining futures in case of a catastrophic error
    if (pendingRowsQueue != null && !pendingRowsQueue.isEmpty()) {
      for (Future<ProcessedRowContext> future : pendingRowsQueue) {
        future.cancel(true);
      }
      pendingRowsQueue.clear();
    }

    if (writerError.get() != null) {
      throw new ModuleException().withMessage("Row writing pipeline aborted").withCause(writerError.get());
    }
  }

  /**
   * Pushes rows into the pipeline, throwing fast if the writer task fails, and
   * using non-deadlocking backpressure.
   */
  private void enqueueRow(Row row) throws ModuleException {
    if (writerError.get() != null) {
      throw new ModuleException().withMessage("Pipeline execution halted due to previous background failure")
        .withCause(writerError.get());
    }

    Callable<ProcessedRowContext> conversionTask = () -> processRowAsync(row);
    Future<ProcessedRowContext> future = executorService.submit(conversionTask);

    try {
      // Prevents deadlocks if the consumer thread crashes while queue is maxed out
      boolean waitingLogged = false;
      while (!pendingRowsQueue.offer(future, 500, TimeUnit.MILLISECONDS)) {
        if (writerError.get() != null) {
          future.cancel(true);
          throw new ModuleException().withMessage("Pipeline halted while enqueuing row").withCause(writerError.get());
        }

        if (!waitingLogged) {
          logger.debug("|| PAUSED: Queue is full. Suspended database reading. Waiting for space to enqueue row [{}]...",
            row.getIndex());
          waitingLogged = true;
        }
      }
      logger.debug(">> ENQUEUED: Row [{}] entered the queue. Current size: {}/{}", row.getIndex(),
        pendingRowsQueue.size(), MAX_QUEUE_SIZE);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.cancel(true);
      throw new ModuleException().withMessage("Row enqueuing process was interrupted").withCause(e);
    }
  }

  /**
   * Processes row columns concurrently on Virtual Threads mapping extracted files
   * for lifecycle control.
   */
  private ProcessedRowContext processRowAsync(Row row) throws ModuleException {
    List<Cell> cells = row.getCells();
    List<Path> transientPaths = new ArrayList<>();

    if (this.conversionService != null) {
      for (int i = 0; i < cells.size(); i++) {
        Cell cell = cells.get(i);

        if (cell instanceof BinaryCell binCell) {
          try (InputStream originalStream = binCell.createInputStream()) {
            ConversionResult result = conversionService.convertLob(cell.getId(), originalStream);

            // TODO: Handle multiple files per cell if needed. Currently assumes single file
            // output.
            Cell newCell = new BinaryCell(cell.getId(), new PathInputStreamProvider(result.zipFile()),
              "application/zip");
            cells.set(i, newCell);

            // Track extracted parts to clean them individually later
            transientPaths.addAll(result.convertedFiles());
            transientPaths.add(result.reportFile());
            transientPaths.add(result.zipFile());
            if (!result.convertedFiles().isEmpty()) {
              transientPaths.add(result.convertedFiles().getFirst().getParent()); // directory container
            }
          } catch (IOException | InterruptedException | HttpLobConversionServiceException e) {
            String statusCodeInfo = "";
            if (e instanceof HttpLobConversionServiceException apiEx && apiEx.getHttpStatusCode() != null) {
              statusCodeInfo = " (HTTP " + apiEx.getHttpStatusCode() + ")";
            }

            String errorMsg = String.format(
              "Conversion failed for cell '%s' in row %d%s. " + "Pipeline continuing with original file. Detail: %s",
              cell.getId(), row.getIndex(), statusCodeInfo, e.getMessage());

            logger.error(errorMsg);
          }
        }
      }
    }
    row.setCells(cells);

    long readyCount = pendingRowsQueue.stream().filter(Future::isDone).count();
    logger.debug("== READY: Row [{}] finished conversion. Currently {} ready rows waiting in queue.", row.getIndex(),
      readyCount + 1);

    return new ProcessedRowContext(row, transientPaths);
  }

  private void cleanupTransientPaths(List<Path> paths) {
    if (paths == null || tempFileTracker == null) {
      return;
    }
    for (Path path : paths) {
      tempFileTracker.deleteEarly(path);
    }
  }

  abstract String getJAXBContext();

  abstract Class<?> getJAXBContextClass();
}