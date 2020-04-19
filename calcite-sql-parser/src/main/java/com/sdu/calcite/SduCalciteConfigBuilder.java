package com.sdu.calcite;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class SduCalciteConfigBuilder {

  private boolean replacesNormRuleSet;
  private List<RelOptRule> normRuleSet;

  private boolean replacesLogicalOptRuleSet;
  private List<RelOptRule> logicalOptRuleSet;

  private boolean replacesLogicalRewriteRuleSet;
  private List<RelOptRule> logicalRewriteRuleSet;

  private RelTrait physicalRelTrait;
  private List<RelOptRule> physicalRuleSet;

  private SduCalciteConfigBuilder() {

  }

  public static SduCalciteConfigBuilder builder() {
    return new SduCalciteConfigBuilder();
  }

  public SduCalciteConfigBuilder setReplacesNormRuleSet(boolean replacesNormRuleSet) {
    this.replacesNormRuleSet = replacesNormRuleSet;
    return this;
  }

  public SduCalciteConfigBuilder addNormRelOptRule(RelOptRule ... rules) {
    if (normRuleSet == null) {
      normRuleSet = new LinkedList<>();
    }
    normRuleSet.addAll(Arrays.asList(rules));
    return this;
  }

  public SduCalciteConfigBuilder setReplacesLogicalOptRuleSet(boolean replacesLogicalOptRuleSet) {
    this.replacesLogicalOptRuleSet = replacesLogicalOptRuleSet;
    return this;
  }

  public SduCalciteConfigBuilder addLogicalRelOptRule(RelOptRule ... rules) {
    if (logicalOptRuleSet == null) {
      logicalOptRuleSet = new LinkedList<>();
    }
    logicalOptRuleSet.addAll(Arrays.asList(rules));
    return this;
  }

  public SduCalciteConfigBuilder setReplacesLogicalRewriteRuleSet(boolean replacesLogicalRewriteRuleSet) {
    this.replacesLogicalRewriteRuleSet = replacesLogicalRewriteRuleSet;
    return this;
  }

  public SduCalciteConfigBuilder addReplacesLogicalRewriteRelOptRule(RelOptRule ... rules) {
    if (logicalRewriteRuleSet == null) {
      logicalRewriteRuleSet = new LinkedList<>();
    }
    logicalRewriteRuleSet.addAll(Arrays.asList(rules));
    return this;
  }

  public SduCalciteConfigBuilder setPhysicalRelTrait(RelTrait physicalRelTrait) {
    this.physicalRelTrait = physicalRelTrait;
    return this;
  }

  public SduCalciteConfigBuilder addPhysicaleRelOptRule(RelOptRule ... rules) {
    if (physicalRuleSet == null) {
      physicalRuleSet = new LinkedList<>();
    }
    physicalRuleSet.addAll(Arrays.asList(rules));
    return this;
  }

  public SduCalciteConfig build() {
    return new SduCalciteConfigImpl(
        replacesNormRuleSet, normRuleSet,
        replacesLogicalOptRuleSet, logicalOptRuleSet,
        replacesLogicalRewriteRuleSet, logicalRewriteRuleSet,
        physicalRelTrait, physicalRuleSet);
  }

  private static class SduCalciteConfigImpl implements SduCalciteConfig {

    private final boolean replacesNormRuleSet;
    private final RuleSet normRuleSet;

    private final boolean replacesLogicalOptRuleSet;
    private final RuleSet logicalOptRuleSet;

    private final boolean replacesLogicalRewriteRuleSet;
    private final RuleSet logicalRewriteRuleSet;

    private final RelTrait physicalRelTrait;
    private final RuleSet physicalRuleSet;

    private SduCalciteConfigImpl(
        boolean replacesNormRuleSet, List<RelOptRule> normRuleSet,
        boolean replacesLogicalOptRuleSet, List<RelOptRule> logicalOptRuleSet,
        boolean replacesLogicalRewriteRuleSet, List<RelOptRule> logicalRewriteRuleSet,
        RelTrait physicalRelTrait, List<RelOptRule> physicalRuleSet) {
      this.replacesNormRuleSet = replacesNormRuleSet;
      this.normRuleSet = ofRulSet(normRuleSet);

      this.replacesLogicalOptRuleSet = replacesLogicalOptRuleSet;
      this.logicalOptRuleSet = ofRulSet(logicalOptRuleSet);

      this.replacesLogicalRewriteRuleSet = replacesLogicalRewriteRuleSet;
      this.logicalRewriteRuleSet = ofRulSet(logicalRewriteRuleSet);

      this.physicalRelTrait = physicalRelTrait;
      this.physicalRuleSet = ofRulSet(physicalRuleSet);
    }

    @Override
    public boolean replacesNormRuleSet() {
      return replacesNormRuleSet;
    }

    @Override
    public RuleSet getNormRuleSet() {
      return normRuleSet;
    }

    @Override
    public boolean replacesLogicalOptRuleSet() {
      return replacesLogicalOptRuleSet;
    }

    @Override
    public RuleSet getLogicalOptRuleSet() {
      return logicalOptRuleSet;
    }

    @Override
    public boolean replacesLogicalRewriteRuleSet() {
      return replacesLogicalRewriteRuleSet;
    }

    @Override
    public RuleSet getLogicalRewriteRuleSet() {
      return logicalRewriteRuleSet;
    }

    @Override
    public RelTrait getPhysicalRelTrait() {
      return physicalRelTrait;
    }

    @Override
    public RuleSet getPhysicalRuleSet() {
      return physicalRuleSet;
    }

    private static RuleSet ofRulSet(List<RelOptRule> rules) {
      if (rules == null || rules.isEmpty()) {
        return null;
      }
      return RuleSets.ofList(rules);
    }
  }

}
