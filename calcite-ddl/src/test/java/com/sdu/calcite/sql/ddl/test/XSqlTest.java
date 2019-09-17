package com.sdu.calcite.sql.ddl.test;

import com.sdu.calcite.sql.parser.XSqlParser;
import java.io.IOException;
import java.io.InputStream;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Before;
import org.junit.Test;

public class XSqlTest {

    private String sqlText;

    @Before
    public void readSqlText() throws IOException {
        InputStream stream = this.getClass().getResourceAsStream("/sql.txt");
        byte[] content = new byte[stream.available()];
        stream.read(content);
        stream.close();

        sqlText = new String(content);
    }

    @Test
    public void testSql() throws Exception {
        SqlNodeList sqlNodes = XSqlParser.parse(sqlText);
        for (SqlNode sqlNode : sqlNodes) {
            System.out.println(sqlNode.toString());
        }
    }

}
