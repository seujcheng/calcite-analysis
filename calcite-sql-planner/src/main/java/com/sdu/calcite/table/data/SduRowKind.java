package com.sdu.calcite.table.data;

public enum SduRowKind {

  INSERT("+I", (byte) 0),

  UPDATE_BEFORE("-U", (byte) 1),

  UPDATE_AFTER("+U", (byte) 2),

  DELETE("-D", (byte) 3);

  private final String shortString;

  private final byte value;

  SduRowKind(String shortString, byte value) {
    this.shortString = shortString;
    this.value = value;
  }

  public String shortString() {
    return shortString;
  }

  public byte toByteValue() {
    return value;
  }


  public static SduRowKind fromByteValue(byte value) {
    switch (value) {
      case 0:
        return SduRowKind.INSERT;

      case 1:
        return SduRowKind.UPDATE_BEFORE;

      case 2:
        return SduRowKind.UPDATE_AFTER;

      case 3:
        return SduRowKind.DELETE;

      default:
        throw new UnsupportedOperationException(
          "Unsupported byte value '" + value + "' for row kind.");
    }
  }

}
