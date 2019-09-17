package com.sdu.calcite.sql.ddl;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.sql.SqlUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * USE FUNCTION function_name AS class_name WITH ( )
 *
 * @author hanhan.zhang
 * @apiNote 必须继承 SqlCall
 */
public class SqlUseFunction extends SqlDdl {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("USE FUNCTION",
      SqlKind.OTHER_DDL);

  private SqlIdentifier functionName;
  private SqlNode className;
  private SqlNodeList funcProps;

  public SqlUseFunction(SqlParserPos pos, SqlIdentifier functionName, SqlNode className,
      SqlNodeList funcProps) {
    super(OPERATOR, pos);

    this.functionName = functionName;
    this.className = className;
    this.funcProps = funcProps;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("USE");
    writer.keyword("FUNCTION");
    functionName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    className.unparse(writer, leftPrec, rightPrec);
    SqlUtils.unparse(writer, funcProps, "WITH");
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(functionName, className, funcProps);
  }

  public String getFunctionName() {
    return functionName.toString();
  }

  public String getFunctionClass() {
    return className.toString();
  }

  public Map<String, String> getFunctionProperties() {
    if (funcProps == null) {
      return Collections.emptyMap();
    }

    Map<String, String> props = new HashMap<>();
    for (SqlNode node : funcProps) {
      SqlPropertyOption propertyNode = (SqlPropertyOption) node;
      props.put(propertyNode.getKeyString(), propertyNode.getValueString());
    }

    return props;
  }
}
