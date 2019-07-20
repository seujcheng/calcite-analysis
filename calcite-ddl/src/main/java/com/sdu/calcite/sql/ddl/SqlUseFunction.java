package com.sdu.calcite.sql.ddl;

import com.google.common.collect.ImmutableList;
import com.sdu.calcite.sql.SqlUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * USE FUNCTION function_name AS class_name WITH (
 * )
 *
 * @apiNote 必须继承 SqlCall
 *
 * @author hanhan.zhang
 * */
public class SqlUseFunction extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("USE FUNCTION", SqlKind.OTHER_DDL);

    private SqlIdentifier functionName;
    private SqlNode className;
    private SqlNodeList properties;

    public SqlUseFunction(SqlParserPos pos, SqlIdentifier functionName, SqlNode className, SqlNodeList properties) {
        super(OPERATOR, pos);

        this.functionName = functionName;
        this.className = className;
        this.properties = properties;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("USE");
        writer.keyword("FUNCTION");
        functionName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        className.unparse(writer, leftPrec, rightPrec);
        SqlUtils.unparse(writer, properties, "WITH");
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(functionName, className, properties);
    }

    public String getFunctionName() {
        return functionName.toString();
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
            SqlPropertyNode propertyNode = (SqlPropertyNode) node;
            props.put(propertyNode.getPropertyName(), propertyNode.getPropertyValue());
        }
        return props;
    }
}
