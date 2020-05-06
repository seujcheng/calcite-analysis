package com.sdu.calcite.api;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.sdu.calcite.plan.catalog.SduCatalog;
import com.sdu.calcite.plan.catalog.SduCatalogTable;
import com.sdu.calcite.plan.catalog.SduObjectIdentifier;
import com.sdu.calcite.plan.catalog.SduObjectPath;
import com.sdu.calcite.plan.catalog.exceptions.SduCatalogException;
import com.sdu.calcite.plan.catalog.exceptions.SduCatalogNotExistException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class SduCatalogManager {


  private Map<String, SduCatalog> catalogs;

  private String currentCatalogName;

  private String currentDatabaseName;

  public SduCatalogManager(String defaultCatalogName, SduCatalog defaultCatalog) {
    checkArgument(isNotBlank(defaultCatalogName), "Default catalog name cannot be null or empty");
    checkNotNull(defaultCatalog, "Default catalog cannot be null");

    catalogs = new LinkedHashMap<>();
    catalogs.put(defaultCatalogName, defaultCatalog);

    this.currentCatalogName = defaultCatalogName;
    this.currentDatabaseName = defaultCatalog.getDefaultDatabase();
  }

  public void registerCatalog(String catalogName, SduCatalog catalog) {
    checkArgument(isNotBlank(catalogName), "Catalog name cannot be null or empty");
    checkNotNull(catalog, "Catalog cannot be null");

    if (catalogs.containsKey(catalogName)) {
      throw new SduCatalogException(format("Catalog %s already exists.", catalogName));
    }

    catalogs.put(catalogName, catalog);
    catalog.open();
  }

  public void setCurrentCatalog(String catalogName) throws SduCatalogNotExistException {
    checkArgument(isNotBlank(catalogName), "Catalog name cannot be null or empty");

    SduCatalog catalog = catalogs.get(catalogName);
    if (catalog == null) {
      throw new SduCatalogNotExistException(catalogName);
    }

    if (!currentCatalogName.equals(catalogName)) {
      currentCatalogName = catalogName;
      currentDatabaseName = catalog.getDefaultDatabase();
    }
  }

  public void setCurrentDatabase(String databaseName) {
    checkArgument(isNotBlank(databaseName), "database name cannot be null or empty");

    if (!catalogs.get(currentCatalogName).databaseExists(databaseName)) {
      throw new SduCatalogException(format(
          "A database with name [%s] does not exist in the catalog: [%s].",
          databaseName,
          currentCatalogName));
    }

    if (!currentDatabaseName.equals(databaseName)) {
      currentDatabaseName = databaseName;
    }
  }

  public Optional<SduCatalog> getCatalog(String catalogName) {
    return Optional.ofNullable(catalogs.get(catalogName));
  }

  public Optional<SduCatalogTable> getTable(SduObjectIdentifier objectIdentifier) {
    SduCatalog catalog = getCatalog(objectIdentifier.getCatalogName())
        .orElseThrow(() -> new SduCatalogNotExistException(objectIdentifier.getCatalogName()));

    SduObjectPath objectPath = objectIdentifier.toObjectPath();

    if (!catalog.tableExists(objectPath)) {
      return Optional.empty();
    }

    return Optional.of(catalog.getTable(objectPath));
  }
}
