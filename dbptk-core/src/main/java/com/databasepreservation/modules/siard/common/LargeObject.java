package com.databasepreservation.modules.siard.common;

import java.nio.file.Path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LargeObject {
  private final ProvidesInputStream inputStream;
  private final String outputPath;

  public LargeObject(ProvidesInputStream inputStream, String outputPath) {
    this.inputStream = inputStream;
    this.outputPath = outputPath;
  }

  public ProvidesInputStream getInputStreamProvider() {
    return inputStream;
  }

  public String getOutputPath() {
    return outputPath;
  }
}
