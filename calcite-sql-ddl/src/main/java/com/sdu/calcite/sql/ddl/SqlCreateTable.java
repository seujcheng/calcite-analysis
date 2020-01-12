package com.sdu.calcite.sql.ddl;

import static com.sdu.calcite.sql.SqlUtils.printIndent;
import static java.util.Objects.requireNonNull;

import com.sdu.calcite.sql.SqlUtils;
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
    private final SqlIdentifier tableName;

    // 表列
    private final SqlNodeList columnList;

    // 属性
    private final SqlNodeList tableProps;

    // 注释
    private final SqlCharStringLiteral comment;

    /** Creates a SqlCreateTable. */
    public SqlCreateTable(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnList, SqlNodeList tableProps, SqlCharStringLiteral comment) {
        super(OPERATOR, pos, false, false);
        this.tableName = requireNonNull(tableName);
        this.columnList = requireNonNull(columnList);
        this.tableProps = tableProps;
        this.comment = comment;
    }

    public String getTableName() {
        return tableName.getSimple();
    }

    public SqlNodeList getColumnList() {
        return columnList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName, columnList, tableProps, comment);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE TABLE");

        // 表名
        tableName.unparse(writer, leftPrec, rightPrec);

        // 表列
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columnList) {
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
        if (tableProps != null) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : tableProps) {
                SqlUtils.printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

}
