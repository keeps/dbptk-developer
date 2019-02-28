/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.exception;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Luis Faria
 */
public class ModuleException extends Exception {

  protected Map<String, Throwable> exceptionMap = null;
  protected String message;

  public ModuleException() {
    super();
  }

  public ModuleException withCause(Throwable cause) {
    initCause(cause);
    return this;
  }

  public ModuleException withExceptionMap(Map<String, Throwable> errors) {
    this.exceptionMap = new HashMap<>(errors);
    return this;
  }

  public ModuleException withMessage(String message) {
    this.message = message;
    return this;
  }

  public <T extends ModuleException> T as(Class<T> subModuleExceptionClass) {
    return (T) this;
  }

  @Deprecated
  public Map<String, Throwable> getExceptionMap() {
    return exceptionMap;
  }

  @Override
  public String getMessage() {
    String mainMessage;
    if (StringUtils.isNotBlank(message)) {
      mainMessage = message;
    } else if (getCause() != null) {
      mainMessage = getCause().getMessage();
    } else {
      mainMessage = getMessage();
    }

    if(StringUtils.isNotBlank(mainMessage)){
      // normalize whitespace and trim all whitespace
      mainMessage = mainMessage.replaceAll("\\n+", "\n").replaceAll("(^\\s+|\\s+$)", "");
    }

    return messagePrefix() + mainMessage;
  }

  @Override
  public String getLocalizedMessage() {
    return getMessage();
  }

  protected String messagePrefix() {
    return "";
  }
}
