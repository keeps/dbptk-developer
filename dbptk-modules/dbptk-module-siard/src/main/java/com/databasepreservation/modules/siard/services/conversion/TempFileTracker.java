package com.databasepreservation.modules.siard.services.conversion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks and manages the lifecycle of temporary files and directories created
 * during the LOB conversion process.
 *
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class TempFileTracker {
  private static final Logger LOGGER = LoggerFactory.getLogger(TempFileTracker.class);
  private final Queue<Path> trackedPaths = new ConcurrentLinkedQueue<>();

  /**
   * Registers a file or directory path to be tracked for final cleanup. * @param
   * path The path to track.
   */
  public void track(Path path) {
    if (path != null) {
      trackedPaths.add(path);
    }
  }

  /**
   * Registers a directory path to be tracked for final cleanup. * @param dirPath
   * The directory path to track.
   */
  public void trackDir(Path dirPath) {
    track(dirPath);
  }

  /**
   * Deletes a tracked path immediately (including non-empty directories) and
   * removes it from the tracking queue to free resources early. * @param path The
   * path to delete immediately.
   */
  public void deleteEarly(Path path) {
    if (path == null) {
      return;
    }
    try {
      deleteRecursively(path);
      trackedPaths.remove(path);
    } catch (Exception e) {
      LOGGER.warn("Unable to delete temporary path early: " + path, e);
    }
  }

  /**
   * Deletes all remaining tracked files and directories comprehensively, ensuring
   * recursive cleanup of nested content.
   */
  public void cleanupAll() {
    for (Path path : trackedPaths) {
      try {
        deleteRecursively(path);
      } catch (Exception e) {
        LOGGER.warn("Unable to delete temporary file/directory during bulk cleanup: " + path, e);
      }
    }
    trackedPaths.clear();
  }

  /**
   * Helper method to perform safe recursive deletion of paths and directories.
   */
  private void deleteRecursively(Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    if (Files.isDirectory(path)) {
      try (Stream<Path> walk = Files.walk(path)) {
        walk.sorted(Comparator.reverseOrder()).forEach(p -> {
          try {
            Files.deleteIfExists(p);
          } catch (IOException e) {
            LOGGER.warn("Failed to delete nested path: " + p, e);
          }
        });
      }
    } else {
      Files.deleteIfExists(path);
    }
  }
}