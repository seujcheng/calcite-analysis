package com.sdu.calcite.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

/**
 * @author hanhan.zhang
 * */
public class SqlUtils {

    private SqlUtils() {}

    public static void unparse(SqlWriter writer, SqlNodeList nodes, String keyword) {
        if (nodes == null) {
            return;
        }
        if (keyword != null && !keyword.isEmpty()) {
            writer.keyword(keyword);
        }
        SqlWriter.Frame frame = writer.startList("(", ")");
        for (SqlNode node : nodes) {
            writer.sep(",");
            node.unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    // 写缩进
    public static void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

}
