package com.sdu.calcite.plan;

import com.sdu.calcite.SduParser;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

public interface SduPlanner {

  SduParser getParser();

  SqlNode validate(SqlNode sqlNode);

  RelNode toRel(SqlNode validated);

  RelNode optimize(RelNode relNode);

}
