package com.databasepreservation.modules.siard.common;

import com.databasepreservation.common.InputStreamProvider;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LargeObject {
  private final InputStreamProvider inputStreamProvider;
  private final String outputPath;

  public LargeObject(InputStreamProvider inputStreamProvider, String outputPath) {
    this.inputStreamProvider = inputStreamProvider;
    this.outputPath = outputPath;
  }

  public InputStreamProvider getInputStreamProvider() {
    return inputStreamProvider;
  }

  public String getOutputPath() {
    return outputPath;
  }
}
