package com.sdu.calcite.api;

public class SduTableException extends RuntimeException{

  public SduTableException(String message) {
    super(message);
  }

  public SduTableException(String message, Throwable cause) {
    super(message, cause);
  }
}
