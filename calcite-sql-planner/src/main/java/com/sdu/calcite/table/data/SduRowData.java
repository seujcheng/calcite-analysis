package com.sdu.calcite.table.data;

public interface SduRowData {

  int getArity();

  SduRowKind getRowKind();

  void setRowKind(SduRowKind kind);

  boolean isNullAt(int pos);

  boolean getBoolean(int pos);

  byte getByte(int pos);

  short getShort(int pos);

  int getInt(int pos);

  long getLong(int pos);

  float getFloat(int pos);

  double getDouble(int pos);

  String getString(int pos);

  byte[] getBinary(int pos);

}

