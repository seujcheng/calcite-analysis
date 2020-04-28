package com.sdu.calcite.plan.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.Convention.Impl;

public class SduConventions {

  private SduConventions() {

  }

  public static final Convention LOGICAL = new Impl("LOGICAL", SduLogicalRel.class);

}
