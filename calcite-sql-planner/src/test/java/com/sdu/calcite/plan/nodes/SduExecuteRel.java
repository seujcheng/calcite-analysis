package com.sdu.calcite.plan.nodes;

import com.sdu.calcite.plan.exec.DataTransformation;
import org.apache.calcite.rel.RelNode;

public interface SduExecuteRel extends RelNode {

  DataTransformation translateToPlanInternal();

}
