/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.common;

import com.databasepreservation.common.io.providers.InputStreamProvider;

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
