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

import com.sdu.calcite.sql.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.*;

/**
 * CREATE FUNCTION function_name AS class_name WITH (
 *    groupId = '',
 *    artifactId = '',
 *    version = ''
 * )
 *
 * @author hanhan.zhang
 * */
public class SqlCreateFunction extends SqlCreate {

  private static final SqlSpecialOperator OPERATOR =
          new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

  private final SqlIdentifier name;
  private final SqlNode className;
  private final SqlNodeList properties;

  /** Creates a SqlCreateFunction. */
  public SqlCreateFunction(SqlParserPos pos, SqlIdentifier name, SqlNode className, SqlNodeList properties) {
    super(OPERATOR, pos, false, false);
    this.name = Objects.requireNonNull(name);
    this.className = className;
    this.properties = properties;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
    writer.keyword("FUNCTION");
    name.unparse(writer, 0, 0);
    writer.keyword("AS");
    className.unparse(writer, 0, 0);
    SqlUtils.unparse(writer, properties, "WITH");
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return Arrays.asList(name, className, properties);
  }

  public String getFunctionName() {
    return name.toString();
  }

  public String getFunctionClass() {
    return className.toString();
  }

  public Map<String, String> getFunctionProperties() {
    if (properties == null) {
      return Collections.emptyMap();
    }
    Map<String, String> props = new HashMap<>();
    for (SqlNode node : properties) {
      SqlOption propertyNode = (SqlOption) node;
      props.put(propertyNode.getKeyString(), propertyNode.getValueString());
    }
    return props;
  }

}
