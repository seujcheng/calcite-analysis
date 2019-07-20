package com.sdu.calcite.sql.ddl;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author hanhan.zhang
 * */
public class SqlPropertyNode extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SQL_PROPERTY", SqlKind.ATTRIBUTE_DEF);

    private final SqlIdentifier key;
    private final SqlNode value;

    public SqlPropertyNode(SqlParserPos pos, SqlIdentifier key, SqlNode value) {
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

    public String getPropertyName() {
        return key.toString();
    }

    public String getPropertyValue() {
        return ((SqlLiteral) value).toValue();
    }
}
