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
