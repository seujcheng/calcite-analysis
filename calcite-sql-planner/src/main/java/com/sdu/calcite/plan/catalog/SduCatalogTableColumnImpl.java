package com.sdu.calcite.plan.catalog;

import java.util.Optional;

public class SduCatalogTableColumnImpl implements SduCatalogTableColumn {

  // 列名
  private final String name;
  // 列类型
  private final String type;
  // 若计算列, 则为表达式; 否则空
  private final String expr;
  // 列注释
  private final String comment;

  public SduCatalogTableColumnImpl(String name, String type, String expr, String comment) {
    this.name = name;
    this.type = type;
    this.expr = expr;
    this.comment = comment;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public Optional<String> getExpr() {
    return Optional.ofNullable(expr);
  }

  @Override
  public Optional<String> getComment() {
    return Optional.ofNullable(comment);
  }

}
