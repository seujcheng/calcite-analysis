package com.sdu.calcite.plan.catalog;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class SduCatalogDatabaseImpl implements SduCatalogDatabase {

  private final Map<String, String> properties;
  private final String comment;

  SduCatalogDatabaseImpl(Map<String, String> properties, @Nullable String comment) {
    this.properties = checkNotNull(properties, "properties cannot be null");
    this.comment = comment;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getComment() {
    return comment;
  }

  @Override
  public SduCatalogDatabase copy() {
    return new SduCatalogDatabaseImpl(new HashMap<>(properties), comment);
  }

}
