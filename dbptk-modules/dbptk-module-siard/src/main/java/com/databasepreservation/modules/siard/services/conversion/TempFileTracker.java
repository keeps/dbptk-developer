package com.databasepreservation.modules.siard.services.conversion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TempFileTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(TempFileTracker.class);
  private final Queue<Path> trackedFiles = new ConcurrentLinkedQueue<>();

  public void track(Path path) {
    trackedFiles.add(path);
  }

  public void trackDir(Path dirPath) {
    trackedFiles.add(dirPath);
  }

  public void cleanupAll() {
    for (Path path : trackedFiles) {
      try {
        Files.deleteIfExists(path);
      } catch (Exception e) {
        LOGGER.warn("Unable to delete temporary file/directory: " + path, e);
      }
    }
    trackedFiles.clear();
  }
}