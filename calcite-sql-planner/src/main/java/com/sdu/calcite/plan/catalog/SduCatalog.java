package com.sdu.calcite.plan.catalog;

import com.sdu.calcite.plan.catalog.exceptions.SduCatalogException;
import com.sdu.calcite.plan.catalog.exceptions.SduTableNotExistException;

/*
 * Catalog管理多个数据库, 每个数据库下有多张数据库表, 每张表通过databaseName.tableName来唯一标识
 * */
public interface SduCatalog {

  void open() throws SduCatalogException;

  void close() throws SduCatalogException;

  boolean databaseExists(String databaseName) throws SduCatalogException;

  boolean tableExists(SduObjectPath tablePath) throws SduCatalogException;

  SduCatalogTable getTable(SduObjectPath tablePath) throws SduTableNotExistException, SduCatalogException;

  String getDefaultDatabase() throws SduCatalogException;

}
