/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * This exception is thrown when a module requires a certain SIARD version.
 * 
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDVersionNotSupportedException extends ModuleException {
  private String versionInfo = null;

  public String getVersionInfo() {
    return versionInfo;
  }

  public SIARDVersionNotSupportedException() {
    super();
  }

  public SIARDVersionNotSupportedException withVersionInfo(String versionInfo) {
    this.versionInfo = versionInfo;
    return this;
  }
}
