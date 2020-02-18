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
public class RequiredParameterException extends ModuleException {
  private String parameter = null;

  public String getParameter() {
    return parameter;
  }

  public RequiredParameterException() {
    super();
  }

  public RequiredParameterException withParameter(String parameter) {
    this.parameter = parameter;
    return this;
  }
}
