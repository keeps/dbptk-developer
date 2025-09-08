package com.databasepreservation.common.io.providers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.databasepreservation.model.exception.ModuleException;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SegmentedPathInputStreamProvider extends PathInputStreamProvider {
  protected boolean isSplit;
  protected ArrayList<Path> partPaths;

  public SegmentedPathInputStreamProvider(Path fileLocation) throws ModuleException {
    if (!Files.isReadable(fileLocation)) {
      // check if it has parts
      partPaths = findFileParts(fileLocation);
      if (partPaths.isEmpty()) {
        throw new ModuleException()
          .withMessage("Path " + fileLocation.toAbsolutePath().toString() + " is not readable.");
      }
      isSplit = true;
    }
    this.path = fileLocation;
  }

  private static ArrayList<Path> findFileParts(Path fileLocation) {
    ArrayList<Path> partPaths = new ArrayList<>();

    String[] pathSplit = fileLocation.toString().split(File.separator);
    Path pathWithoutSegment = Paths
      .get(String.join(File.separator, java.util.Arrays.copyOf(pathSplit, pathSplit.length - 2)));
    int initialSegment = Integer.parseInt(pathSplit[pathSplit.length - 2].replaceAll("[^0-9]", ""));

    int currentPart = 0;
    boolean canHaveNextPart = true;
    boolean foundPart = false;
    for (int segment = initialSegment; canHaveNextPart; segment++, foundPart = false) {
      Path filePartPath = pathWithoutSegment.resolve("seg_" + segment + File.separator + fileLocation.getFileName()
        + "_part" + String.format("%03d", currentPart + 1));
      for (int part = currentPart; Files.exists(filePartPath); part++, filePartPath = pathWithoutSegment.resolve("seg_"
        + segment + File.separator + fileLocation.getFileName() + "_part" + String.format("%03d", currentPart + 1))) {
        foundPart = true;
        currentPart++;
        partPaths.add(filePartPath);
      }
      if (!foundPart) {
        canHaveNextPart = false;
      }
    }

    return partPaths;
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    try {
      if (!isSplit) {
        return Files.newInputStream(path);
      } else {
        InputStream partsStream = Files.newInputStream(partPaths.get(0));
        for (int i = 1; i < partPaths.size(); i++) {
          if (!Files.exists(partPaths.get(i))) {
            throw new ModuleException()
              .withMessage("Part file " + partPaths.get(i).toAbsolutePath().toString() + " does not exist.");
          }
          partsStream = new SequenceInputStream(partsStream, Files.newInputStream(partPaths.get(i)));
        }
        return partsStream;
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not create an input stream").withCause(e);
    }
  }

  @Override
  public long getSize() throws ModuleException {
    try {
      if (!isSplit) {
        return Files.size(path);
      } else {
        long total = 0;
        for (int i = 0; i < partPaths.size(); i++) {
          if (!Files.exists(partPaths.get(i))) {
            throw new ModuleException()
              .withMessage("Part file " + partPaths.get(i).toAbsolutePath().toString() + " does not exist.");
          }
          total += Files.size(partPaths.get(i));
        }
        return total;
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not get file size").withCause(e);
    }
  }
}
