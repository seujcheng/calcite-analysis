package com.sdu.calcite.sql.ddl.test;

import com.sdu.calcite.sql.parser.XSqlParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

public class XSqlTest {


    @Test
    public void createTable() throws Exception {
        String sql = "CREATE TABLE Student (id INT(11), name VARCHAR(25)) WITH (" +
                "type = 'kafka', is_sink_table = false)";
        SqlNode node = XSqlParser.parseOne(sql);

        System.out.println(node.getKind());
    }

}
