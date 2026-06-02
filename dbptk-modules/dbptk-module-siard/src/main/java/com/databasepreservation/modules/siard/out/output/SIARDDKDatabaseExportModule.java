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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.PathInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKContextDocumentationWriter;
import com.databasepreservation.modules.siard.out.metadata.SIARDDKFileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.metadata.SIARDMarshaller;
import com.databasepreservation.modules.siard.services.conversion.ConversionResult;
import com.databasepreservation.modules.siard.services.conversion.HttpLobConversionService;
import com.databasepreservation.modules.siard.services.conversion.LobConversionService;
import com.databasepreservation.modules.siard.services.conversion.TempFileTracker;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKDatabaseExportModule extends SIARDExportDefault {

  private SIARDDKExportModule siarddkExportModule;
  private static final Logger logger = LoggerFactory.getLogger(SIARDDKDatabaseExportModule.class);

  private ExecutorService executorService;
  private Queue<Future<Row>> pendingRowsQueue;
  private TempFileTracker tempFileTracker;
  private LobConversionService conversionService;
  private static final int MAX_QUEUE_SIZE = 100;

  public SIARDDKDatabaseExportModule(SIARDDKExportModule siarddkExportModule) {
    super(siarddkExportModule.getContentExportStrategy(), siarddkExportModule.getMainContainer(),
      siarddkExportModule.getWriteStrategy(), siarddkExportModule.getMetadataExportStrategy(), null);

    this.siarddkExportModule = siarddkExportModule;
  }

  @Override
  public void initDatabase() throws ModuleException {
    super.initDatabase();

    this.tempFileTracker = new TempFileTracker();
    String apiEndpoint = "http://localhost:8080";
    String targetFormat = "image/tiff";
    this.conversionService = new HttpLobConversionService(apiEndpoint, targetFormat, this.tempFileTracker);
    this.executorService = Executors.newVirtualThreadPerTaskExecutor();

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
    // Prepare the FIFO queue for the new table
    this.pendingRowsQueue = new LinkedList<>();
    super.handleDataOpenTable(tableId);
  }

  @Override
  public void handleDataRow(Row row) throws ModuleException {
    // 1. Submit the row conversion to the Virtual Thread
    Callable<Row> conversionTask = () -> processRow(row);
    Future<Row> futureRow = executorService.submit(conversionTask);
    pendingRowsQueue.add(futureRow);

    logger.debug("Submitted row for asynchronous processing. Current queue size: {}", pendingRowsQueue.size());
    // 2. Backpressure: Wait for the queue to drain if it reaches the limit
    while (pendingRowsQueue.size() >= MAX_QUEUE_SIZE) {
      logger.debug("Pending rows queue has reached the maximum size of {}. Waiting for the oldest task to complete...",
        MAX_QUEUE_SIZE);
      drainHeadAndExport();
    }
  }

  @Override
  public void handleDataCloseTable(String tableId) throws ModuleException {
    // Process and export all remaining rows in the queue
    while (pendingRowsQueue != null && !pendingRowsQueue.isEmpty()) {
      drainHeadAndExport();
    }
    super.handleDataCloseTable(tableId);
  }

  @Override
  public void finishDatabase() throws ModuleException {
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
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

      siardMarshaller.marshal(getJAXBContextClass(),
        metadataPathStrategy.getXsdResourcePath(SIARDDKConstants.FILE_INDEX),
        "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/fileIndex.xsd", writer,
        SIARDDKFileIndexFileStrategy.generateXML(null));

      writer.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error writing fileIndex to the archive.").withCause(e);
    }

  }

  abstract String getJAXBContext();

  abstract Class<?> getJAXBContextClass();

  private void drainHeadAndExport() throws ModuleException {
    Future<Row> oldestFuture = pendingRowsQueue.poll();
    if (oldestFuture != null) {
      try {
        logger.debug("Waiting for the oldest row conversion task to complete. Remaining queue size after polling: {}",
          pendingRowsQueue.size());
        Row processedRow = oldestFuture.get();

        logger.debug("Oldest row conversion task completed. Exporting row with ID: {}", processedRow.getIndex());

        super.handleDataRow(processedRow);
      } catch (InterruptedException | ExecutionException e) {
        oldestFuture.cancel(true);
        throw new ModuleException().withMessage("Error processing row conversion task").withCause(e);
      }
    }
  }

  private Row processRow(Row row) throws Exception {
    logger.debug("Processing row with ID: {} in thread: {}", row.getIndex(), Thread.currentThread().getName());
    List<Cell> cells = row.getCells();

    for (int i = 0; i < cells.size(); i++) {
      Cell cell = cells.get(i);

      if (cell instanceof BinaryCell binCell) {
        try (InputStream originalStream = binCell.createInputStream()) {
          ConversionResult result = conversionService.convertLob(cell.getId(), originalStream);
          Cell newCell = new BinaryCell(cell.getId(), new PathInputStreamProvider(result.convertedFile()),
            "image/tiff");
          cells.set(i, newCell);
        }
      }
    }
    row.setCells(cells);
    logger.debug("Completed processing row with ID: {}", row.getIndex());
    return row;
  }
}
