/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.exception;

/**
 * This exception is thrown when a module requires that the user accepts a
 * licenseInfo but the user does not do that explicitly beforehand.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LicenseNotAcceptedException extends ModuleException {
  private String licenseInfo = null;

  public String getLicenseInfo() {
    return licenseInfo;
  }

  public LicenseNotAcceptedException() {
    super();
  }

  public LicenseNotAcceptedException withLicenseInfo(String licenseInfo) {
    this.licenseInfo = licenseInfo;
    return this;
  }
}
