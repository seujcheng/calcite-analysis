package com.sdu.calcite.plan.catalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SduCatalogTableImpl implements SduCatalogTable {

  private final List<SduCatalogTableColumn> columns;
  private final Optional<String> comment;
  private final Optional<Map<String, String>> tableProps;

  public SduCatalogTableImpl(
      List<SduCatalogTableColumn> columns,
      String comment,
      Map<String, String> tableProps) {
    this.columns = columns;
    this.comment = Optional.ofNullable(comment);
    this.tableProps = Optional.ofNullable(tableProps);
  }

  @Override
  public List<SduCatalogTableColumn> getColumns() {
    return columns;
  }

  @Override
  public Optional<Map<String, String>> getProperties() {
    return tableProps;
  }

  @Override
  public Optional<String> getComment() {
    return comment;
  }

  @Override
  public SduCatalogTable copy() {
    return new SduCatalogTableImpl(
        columns,
        comment.orElse(null),
        tableProps.orElse(null)
    );
  }

}
