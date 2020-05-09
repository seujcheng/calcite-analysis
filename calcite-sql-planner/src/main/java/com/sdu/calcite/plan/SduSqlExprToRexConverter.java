package com.sdu.calcite.plan;

import org.apache.calcite.rex.RexNode;

public interface SduSqlExprToRexConverter {

  RexNode convertToRexNode(String expr);

  RexNode[] convertToRexNodes(String[] expressions);

}
