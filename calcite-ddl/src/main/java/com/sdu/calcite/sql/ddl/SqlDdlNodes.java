/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sdu.calcite.sql.ddl;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {

  private SqlDdlNodes() {}

  // -------------------------------------------------------------------------
  static void unparser(SqlWriter writer, SqlNodeList sqlNodes) {
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode c : sqlNodes) {
      writer.sep(",");
      c.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  public static SqlNode check(SqlParserPos pos, SqlIdentifier name, SqlNode expression) {
    return new SqlCheckConstraint(pos, name, expression);
  }

  public static SqlKeyConstraint unique(SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList);
  }

  public static SqlKeyConstraint primary(SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList) {
    return new SqlKeyConstraint(pos, name, columnList) {
      @Override
      public SqlOperator getOperator() {
        return PRIMARY;
      }
    };
  }

  public static SqlCreateFunction createFunction(
          SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name,
          SqlNode className, SqlNodeList usingList) {
    return new SqlCreateFunction(pos, replace, ifNotExists, name,
            className, usingList);
  }

  public static SqlNode column(
          SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, ColumnStrategy strategy) {
    return new SqlColumnDeclaration(pos, name, dataType, expression, strategy);
  }

  public static Pair<CalciteSchema, String> schema(
          CalcitePrepare.Context context, boolean mutable, SqlIdentifier id) {

    final String name;
    final List<String> path;

    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }

    CalciteSchema schema = mutable ? context.getMutableRootSchema() : context.getRootSchema();

    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }

    return Pair.of(schema, name);
  }

  static void populate(
          SqlIdentifier name, SqlNode query, CalcitePrepare.Context context) {

    // Generate, prepare and execute an "INSERT INTO table query" statement.
    // (It's a bit inefficient that we convert from SqlNode to SQL and back
    // again.)
    final FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(context.getRootSchema().plus())
            .build();
    final Planner planner = Frameworks.getPlanner(config);
    try {
      final StringWriter sw = new StringWriter();
      final PrintWriter pw = new PrintWriter(sw);
      final SqlPrettyWriter w =
              new SqlPrettyWriter(CalciteSqlDialect.DEFAULT, false, pw);
      pw.print("INSERT INTO ");
      name.unparse(w, 0, 0);
      pw.print(" ");
      query.unparse(w, 0, 0);
      pw.flush();
      final String sql = sw.toString();
      final SqlNode query1 = planner.parse(sql);
      final SqlNode query2 = planner.validate(query1);
      final RelRoot r = planner.rel(query2);
      final PreparedStatement prepare = context.getRelRunner().prepare(r.rel);
      int rowCount = prepare.executeUpdate();
      Util.discard(rowCount);
      prepare.close();
    } catch (SqlParseException | ValidationException
            | RelConversionException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public enum FileType {
    FILE,
    JAR,
    ARCHIVE
  }

}

// End SqlDdlNodes.java
