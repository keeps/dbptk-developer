package com.databasepreservation.modules.siard.services.conversion;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class HttpLobConversionServiceException extends Exception {

  private final Integer httpStatusCode;

  public HttpLobConversionServiceException(String message) {
    super(message);
    this.httpStatusCode = null;
  }

  public HttpLobConversionServiceException(String message, Throwable cause) {
    super(message, cause);
    this.httpStatusCode = null;
  }

  public HttpLobConversionServiceException(String message, int httpStatusCode) {
    super(message);
    this.httpStatusCode = httpStatusCode;
  }

  public HttpLobConversionServiceException(String message, int httpStatusCode, Throwable cause) {
    super(message, cause);
    this.httpStatusCode = httpStatusCode;
  }

  public Integer getHttpStatusCode() {
    return httpStatusCode;
  }
}
