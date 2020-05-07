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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateFunction extends SqlCreate {

  private static final SqlSpecialOperator OPERATOR =
          new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

  private final SqlIdentifier functionIdentifier;

  private final SqlNodeList functionProps;

  public SqlCreateFunction(
      SqlParserPos pos,
      SqlIdentifier functionIdentifier,
      SqlNodeList functionProps) {
    super(OPERATOR, pos, false, false);
    this.functionIdentifier = requireNonNull(functionIdentifier);
    this.functionProps = requireNonNull(functionProps);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE FUNCTION");
    functionIdentifier.unparse(writer, leftPrec, rightPrec);
    writer.keyword("WITH");
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode node : functionProps) {
      writer.sep(",");
      node.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(functionIdentifier, functionProps);
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public String getName() {
    return getFunctionIdentifier()[getFunctionIdentifier().length - 1];
  }

  public String[] getFunctionIdentifier() {
    return functionIdentifier.names.toArray(new String[0]);
  }

  public SqlNodeList getProperties() {
    return functionProps;
  }

}
