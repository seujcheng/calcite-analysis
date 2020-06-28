package com.sdu.calcite.table.types;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class SduLogicalType implements Serializable {

  private final boolean isNullable;

  private final SduLogicalTypeRoot typeRoot;

  public SduLogicalType(boolean isNullable, SduLogicalTypeRoot typeRoot) {
    this.isNullable = isNullable;
    this.typeRoot = Preconditions.checkNotNull(typeRoot);
  }

  public boolean isNullable() {
    return isNullable;
  }

  public SduLogicalTypeRoot getTypeRoot() {
    return typeRoot;
  }

  public abstract List<SduLogicalType> getChildren();

  public abstract boolean supportsInputConversion(Class<?> clazz);

  public abstract boolean supportsOutputConversion(Class<?> clazz);

  // 访问者模式
  public abstract <R> R accept(SduLogicalTypeVisitor<R> visitor);


  protected static Set<String> conversionSet(String... elements) {
    return new HashSet<>(Arrays.asList(elements));
  }

}
