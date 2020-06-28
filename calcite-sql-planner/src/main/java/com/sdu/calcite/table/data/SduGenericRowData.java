package com.sdu.calcite.table.data;

public class SduGenericRowData implements SduRowData {

  private final Object[] fields;

  private SduRowKind kind;

  public SduGenericRowData(int arity) {
    this.kind = SduRowKind.INSERT;
    this.fields = new Object[arity];
  }

  public SduGenericRowData(SduRowKind kind, int arity) {
    this.kind = kind;
    this.fields = new Object[arity];
  }

  public void setField(int pos, Object value) {
    this.fields[pos] = value;
  }

  public Object getField(int pos) {
    return this.fields[pos];
  }

  @Override
  public int getArity() {
    return fields.length;
  }

  @Override
  public SduRowKind getRowKind() {
    return kind;
  }

  @Override
  public void setRowKind(SduRowKind kind) {
    this.kind = kind;
  }

  @Override
  public boolean isNullAt(int pos) {
    return this.fields[pos] == null;
  }

  @Override
  public boolean getBoolean(int pos) {
    return (boolean) this.fields[pos];
  }

  @Override
  public byte getByte(int pos) {
    return (byte) this.fields[pos];
  }

  @Override
  public short getShort(int pos) {
    return (short) this.fields[pos];
  }

  @Override
  public int getInt(int pos) {
    return (int) this.fields[pos];
  }

  @Override
  public long getLong(int pos) {
    return (long) this.fields[pos];
  }

  @Override
  public float getFloat(int pos) {
    return (float) this.fields[pos];
  }

  @Override
  public double getDouble(int pos) {
    return (double) this.fields[pos];
  }

  @Override
  public String getString(int pos) {
    return (String) this.fields[pos];
  }

  @Override
  public byte[] getBinary(int pos) {
    return (byte[]) this.fields[pos];
  }

}
