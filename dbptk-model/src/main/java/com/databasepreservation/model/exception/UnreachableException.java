package com.databasepreservation.model.exception;

/**
 * Some methods, under very specific circumstances, will never throw exceptions
 * even though they are declared.
 *
 * If you want a way to avoid having to handle the Exception that probably will
 * never be thrown, but you still want to have a way to be notified if it
 * somehow it is thrown, then enclose the Exception in an UnreachableException.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class UnreachableException extends RuntimeException {
  public UnreachableException() {
  }

  public UnreachableException(String s) {
    super(s);
  }

  public UnreachableException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public UnreachableException(Throwable throwable) {
    super(throwable);
  }

  public UnreachableException(String s, Throwable throwable, boolean b, boolean b1) {
    super(s, throwable, b, b1);
  }
}
