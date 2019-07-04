package com.sdu.calcite.sql.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class SqlProperty extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SQL_PRO", SqlKind.DESCRIBE_TABLE);

    private final SqlIdentifier key;
    private final SqlNode value;

    public SqlProperty(SqlParserPos pos, SqlIdentifier key, SqlNode value) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    public String getSqlPropertyName() {
        return key.toString();
    }

    public String getSqlPropertyValue() {
        return ((SqlLiteral) value).toValue();
    }
}
