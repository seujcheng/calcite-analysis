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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

  // ------------ catalog ------------
  public boolean schemaExists(String catalogName) {
    return getCatalog(catalogName).isPresent();
  }

  public Set<String> listCatalogs() {
    return Collections.unmodifiableSet(catalogs.keySet());
  }

  private Optional<SduCatalog> getCatalog(String catalogName) {
    return Optional.ofNullable(catalogs.get(catalogName));
  }

  // ------------ catalog database -----------
  public boolean schemaExists(String catalogName, String databaseName) {
    return getCatalog(catalogName)
        .map(catalog -> catalog.databaseExists(databaseName))
        .orElse(false);
  }

  public Set<String> listSchemas(String catalogName) {
    Set<String> databases = new HashSet<>();
    getCatalog(catalogName)
        .ifPresent(catalog -> databases.addAll(catalog.listDatabases()));
    return databases;
  }

  // ------------ catalog table ------------

  public Set<String> listTables(String catalogName, String databaseName) {
    Set<String> tables = new HashSet<>();
    getCatalog(catalogName)
        .filter(catalog -> catalog.databaseExists(databaseName))
        .ifPresent(catalog -> tables.addAll(catalog.listTables(databaseName)));

    return tables;
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
