/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SiardNotFoundException extends ModuleException {

  private String path;

  public SiardNotFoundException() {
    super();
  }

  public SiardNotFoundException(String message) {
    this();
    withMessage(message);
  }

  public String getPath() {
    return this.path;
  }

  public SiardNotFoundException withPath(String path) {
    this.path = path;
    return this;
  }
}
