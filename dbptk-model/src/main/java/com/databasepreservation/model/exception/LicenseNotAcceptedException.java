package com.databasepreservation.model.exception;

/**
 * This exception is thrown when a module requires that the user accepts a
 * license but the user does not do that explicitly beforehand.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class LicenseNotAcceptedException extends Exception {
  private final String license;

  /**
   * Create the exception, providing it with the license text
   * 
   * @param license
   *          the license text that should be shown if the license was not
   *          accepted
   */
  public LicenseNotAcceptedException(String license) {
    this.license = license;
  }

  /**
   * @return the license string
   */
  public String getLicense() {
    return license;
  }
}
