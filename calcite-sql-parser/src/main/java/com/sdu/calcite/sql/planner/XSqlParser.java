package com.sdu.calcite.sql.planner;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author hanhan.zhang
 * */
public class XSqlParser {

    private static final SqlParser.Config config;

    static {
        config = SqlParser.configBuilder()
                .setCaseSensitive(true)
                // 禁止转为大写
                .setUnquotedCasing(Casing.UNCHANGED)
                .setParserFactory(new XSqlParserImplFactory())
                .build();
    }

    public static SqlNodeList parse(String text) throws Exception {
        SqlParser parser = SqlParser.create(text, config);
        return parser.parseStmtList();
    }

    public static SqlNode parseOne(String text) throws Exception {
        SqlParser parser = SqlParser.create(text, config);
        return parser.parseQuery(text);
    }

}
