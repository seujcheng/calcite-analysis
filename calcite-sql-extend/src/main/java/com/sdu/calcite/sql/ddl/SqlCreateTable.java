package com.sdu.calcite.sql.ddl;

import static java.util.Objects.requireNonNull;

import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * @author hanhan.zhang
 * */
public class SqlCreateTable extends SqlCreate  {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    // 表名
    private final SqlIdentifier name;
    // 表列
    private final SqlNodeList columns;
    // 属性
    private final SqlNodeList properties;
    // 注释
    private final SqlCharStringLiteral comment;

    public SqlCreateTable(
        SqlParserPos pos,
        SqlIdentifier name,
        SqlNodeList columns,
        SqlCharStringLiteral comment,
        SqlNodeList properties) {
        super(OPERATOR, pos, false, false);
        this.name = requireNonNull(name, "table name should not be null");
        this.columns = requireNonNull(columns, "table columns should not be null");
        this.comment = comment;
        this.properties = properties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public SqlIdentifier getName() {
        return name;
    }

    public String[] fullTableName() {
        return name.names.toArray(new String[0]);
    }

    public SqlNodeList getColumns() {
        return columns;
    }

    public SqlCharStringLiteral getComment() {
        return comment;
    }

    public SqlNodeList getProperties() {
        return properties;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, columns, comment, properties);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE TABLE");

        // 表名
        name.unparse(writer, leftPrec, rightPrec);

        // 表列
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columns) {
            printIndent(writer);
            if (column instanceof SqlBasicCall) {
                SqlCall call = (SqlCall) column;
                SqlCall newCall = call.getOperator().createCall(SqlParserPos.ZERO, call.operand(1), call.operand(0));
                newCall.unparse(writer, leftPrec, rightPrec);
            } else {
                column.unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.newlineAndIndent();
        writer.endList(frame);

        // 注释
        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        // 属性
        if (properties != null) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : properties) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    // 写缩进
    private static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }
}
