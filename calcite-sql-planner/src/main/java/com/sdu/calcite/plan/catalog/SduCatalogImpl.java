package com.sdu.calcite.plan.catalog;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.sdu.calcite.plan.catalog.exceptions.SduCatalogException;
import com.sdu.calcite.plan.catalog.exceptions.SduDatabaseAlreadyExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduDatabaseNotExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduFunctionAlreadyExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduTableAlreadyExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduTableNotExistException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class SduCatalogImpl implements SduCatalog {

  private static final String DEFAULT_DB = "default";

  private final String catalogName;
  private final String defaultDatabase = DEFAULT_DB;

  private Map<String, SduCatalogDatabase> databases;

  private Map<SduObjectPath, SduCatalogTable> tables;

  private Map<SduObjectPath, SduCatalogFunction> functions;

  public SduCatalogImpl(String catalogName) {
    this.catalogName = catalogName;
    this.databases = new LinkedHashMap<>();
    this.databases.put(DEFAULT_DB, new SduCatalogDatabaseImpl(new HashMap<>(), null));

    this.tables = new LinkedHashMap<>();
    this.functions = new LinkedHashMap<>();
  }

  @Override
  public void open() throws SduCatalogException {

  }

  @Override
  public void close() throws SduCatalogException {

  }

  @Override
  public void createDatabase(String databaseName, SduCatalogDatabase database, boolean ignoreIfExists)
      throws SduDatabaseAlreadyExistException, SduCatalogException {
    checkArgument(StringUtils.isNotEmpty(databaseName));
    checkNotNull(database);

    if (databaseExists(databaseName)) {
      if (!ignoreIfExists) {
        throw new SduDatabaseAlreadyExistException(catalogName, databaseName);
      }
    } else {
      databases.put(databaseName, database.copy());
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws SduCatalogException {
    checkArgument(StringUtils.isNoneEmpty(databaseName));

    return databases.containsKey(databaseName);
  }

  @Override
  public List<String> listDatabases() throws SduCatalogException {
    return new ArrayList<>(databases.keySet());
  }

  @Override
  public void createTable(SduObjectPath tablePath, SduCatalogTable catalogTable,
      boolean ignoreIfExists)
      throws SduTableAlreadyExistException, SduDatabaseNotExistException, SduCatalogException {
    checkNotNull(tablePath);
    checkNotNull(catalogTable);

    /*
     * 1: 判断所属的数据库是否存在
     *
     * 2: 判断数据库表是否已经存在
     * */
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new SduDatabaseNotExistException(catalogName, tablePath.getDatabaseName());
    }

    if (tableExists(tablePath)) {
      if (!ignoreIfExists) {
        throw new SduTableAlreadyExistException(catalogName, tablePath);
      }
    }

    tables.put(tablePath, catalogTable);
  }

  @Override
  public boolean tableExists(SduObjectPath tablePath) throws SduCatalogException {
    checkNotNull(tablePath);

    return databaseExists(tablePath.getDatabaseName()) && tables.containsKey(tablePath);
  }

  @Override
  public List<String> listTables(String databaseName)
      throws SduDatabaseNotExistException, SduCatalogException {
    checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");

    if (!databaseExists(databaseName)) {
      throw new SduDatabaseNotExistException(catalogName, databaseName);
    }

    return tables.keySet().stream()
        .filter(objectPath -> objectPath.getDatabaseName().equals(databaseName))
        .map(SduObjectPath::getObjectName)
        .collect(Collectors.toList());
  }

  @Override
  public SduCatalogTable getTable(SduObjectPath tablePath)
      throws SduTableNotExistException, SduCatalogException {
    checkNotNull(tablePath);

    if (!tableExists(tablePath)) {
      throw new SduTableNotExistException(catalogName, tablePath);
    }

    return tables.get(tablePath).copy();
  }

  @Override
  public String getDefaultDatabase() throws SduCatalogException {
    return defaultDatabase;
  }

  @Override
  public boolean functionExists(SduObjectPath path) {
    checkNotNull(path);

    SduObjectPath functionPath = normalize(path);

    return databaseExists(functionPath.getDatabaseName()) && functions.containsKey(functionPath);
  }

  @Override
  public void createFunction(SduObjectPath functionPath, SduCatalogFunction function,
      boolean ignoreIfExists)
      throws SduFunctionAlreadyExistException, SduDatabaseNotExistException, SduCatalogException {

    checkNotNull(functionPath);
    checkNotNull(function);

    if (!databaseExists(functionPath.getDatabaseName())) {
      throw new SduDatabaseNotExistException(catalogName, functionPath.getDatabaseName());
    }

    if (functionExists(functionPath)) {
      if (!ignoreIfExists) {
        throw new SduFunctionAlreadyExistException(catalogName, functionPath);
      }
    } else {
      functions.put(functionPath, function.copy());
    }
  }

  private SduObjectPath normalize(SduObjectPath path) {
    return SduObjectPath.of(path.getDatabaseName(), path.getObjectName().toLowerCase());
  }
}
