package com.sdu.calcite.plan.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

public class InputRefVisitor extends RexVisitorImpl<Void> {

  private final Set<Integer> fieldIndexes;

  public InputRefVisitor() {
    super(true);
    this.fieldIndexes = new LinkedHashSet<>();
  }

  @Override
  public Void visitInputRef(RexInputRef inputRef) {
    fieldIndexes.add(inputRef.getIndex());
    return null;
  }

  @Override
  public Void visitCall(RexCall call) {
    call.getOperands()
        .forEach(rexNode -> rexNode.accept(this));
    return null;
  }

  public List<Integer> getFieldIndexes() {
    return new ArrayList<>(fieldIndexes);
  }
}
