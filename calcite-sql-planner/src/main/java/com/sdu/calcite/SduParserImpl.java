package com.sdu.calcite;

import com.sdu.calcite.plan.SduPlannerContext;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class SduParserImpl implements SduParser {

  private final SduPlannerContext plannerContext;

  public SduParserImpl(SduPlannerContext plannerContext) {
    this.plannerContext = plannerContext;
  }

  @Override
  public SqlNodeList parseStmtList(String stmt) {
    return plannerContext.parseStmtList(stmt);
  }

  @Override
  public SqlNode parseStmt(String stmt) {
    return plannerContext.parseStmt(stmt);
  }

}
