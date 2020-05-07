package com.sdu.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public interface SduParser {

  SqlNodeList parseStmtList(String stmt);

  SqlNode parseStmt(String stmt);

}
