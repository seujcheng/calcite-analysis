package com.sdu.calcite.plan.catalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SduCatalogTableImpl implements SduCatalogTable {

  private final List<SduCatalogTableColumn> columns;
  private final Optional<SduCatalogTableWatermark> tableWatermark;
  private final Optional<String> comment;
  private final Optional<Map<String, String>> tableProps;

  public SduCatalogTableImpl(
      List<SduCatalogTableColumn> columns,
      SduCatalogTableWatermark watermark,
      String comment,
      Map<String, String> tableProps) {
    this.columns = columns;
    this.tableWatermark = Optional.ofNullable(watermark);
    this.comment = Optional.ofNullable(comment);
    this.tableProps = Optional.ofNullable(tableProps);
  }

  @Override
  public List<SduCatalogTableColumn> getColumns() {
    return columns;
  }

  @Override
  public Optional<SduCatalogTableWatermark> getTableWatermark() {
    return tableWatermark;
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
        tableWatermark.orElse(null),
        comment.orElse(null),
        tableProps.orElse(null)
    );
  }

}
