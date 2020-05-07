package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.plan.catalog.exceptions.SduCatalogException;
import com.sdu.calcite.plan.catalog.exceptions.SduDatabaseNotExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduFunctionAlreadyExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduTableAlreadyExistException;
import com.sdu.calcite.plan.catalog.exceptions.SduTableNotExistException;
import java.util.List;

/*
 * Catalog管理多个数据库, 每个数据库下有多张数据库表, 每张表通过databaseName.tableName来唯一标识
 * */
public interface SduCatalog {

  void open() throws SduCatalogException;

  void close() throws SduCatalogException;

  // ------- database ----------
  boolean databaseExists(String databaseName) throws SduCatalogException;

  List<String> listDatabases() throws SduCatalogException;

  // ------- table -----------

  void createTable(SduObjectPath tablePath, SduCatalogTable catalogTable, boolean ignoreIfExists)
      throws SduTableAlreadyExistException, SduDatabaseNotExistException, SduCatalogException;

  boolean tableExists(SduObjectPath tablePath) throws SduCatalogException;

  List<String> listTables(String databaseName) throws SduDatabaseNotExistException, SduCatalogException;

  SduCatalogTable getTable(SduObjectPath tablePath) throws SduTableNotExistException, SduCatalogException;

  String getDefaultDatabase() throws SduCatalogException;

  // ------- function ---------
  // ------------- catalog function -------------
  boolean functionExists(SduObjectPath path);

  void createFunction(SduObjectPath functionPath, SduCatalogFunction function, boolean ignoreIfExists)
      throws SduFunctionAlreadyExistException, SduDatabaseNotExistException, SduCatalogException;


}
