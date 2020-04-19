package com.sdu.calcite;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.tools.RuleSet;

public interface SduCalciteConfig {

  boolean replacesNormRuleSet();

  RuleSet getNormRuleSet();

  boolean replacesLogicalOptRuleSet();

  RuleSet getLogicalOptRuleSet();

  boolean replacesLogicalRewriteRuleSet();

  RuleSet getLogicalRewriteRuleSet();

  RelTrait getPhysicalRelTrait();

  RuleSet getPhysicalRuleSet();

}
