package com.sdu.calcite.sql.ddl;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

public class SqlOption extends SqlCall {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("SQL_PROPERTY", SqlKind.ATTRIBUTE_DEF);

    private final SqlNode key;
    private final SqlNode value;

    public SqlOption(SqlParserPos pos, SqlNode key, SqlNode value) {
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
        return ImmutableNullableList.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    public String getKeyString() {
        return ((NlsString)(SqlLiteral.value(key))).getValue();
    }

    public String getValueString() {
        return ((NlsString)(SqlLiteral.value(value))).getValue();
    }

}
