package com.databasepreservation.modules.siard.common;

import com.databasepreservation.common.ProvidesInputStream;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LargeObject {
  private final ProvidesInputStream providesInputStream;
  private final String outputPath;

  public LargeObject(ProvidesInputStream providesInputStream, String outputPath) {
    this.providesInputStream = providesInputStream;
    this.outputPath = outputPath;
  }

  public ProvidesInputStream getInputStreamProvider() {
    return providesInputStream;
  }

  public String getOutputPath() {
    return outputPath;
  }
}
