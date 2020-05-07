package com.sdu.calcite.api;

public class SduValidationException extends RuntimeException {

  public SduValidationException(String message) {
    super(message);
  }

  public SduValidationException(String message, Throwable cause) {
    super(message, cause);
  }
}
