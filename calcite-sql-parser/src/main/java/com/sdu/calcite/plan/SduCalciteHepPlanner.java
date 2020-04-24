package com.sdu.calcite.plan;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;


class SduCalciteHepPlanner {

  private SduCalciteHepPlanner() {

  }

  /**
   * run HEP planner with rules applied simultaneously. Apply all of the rules to the given
   * node before going to the next one. If a rule creates a new node all of the rules will
   * be applied to this new node.
   * */
  static RelNode runHepPlannerSimultaneously(
      HepMatchOrder hepMatchOrder ,
      RuleSet ruleSet,
      RelNode input,
      RelTraitSet targetTraits,
      Context context) {

    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addMatchOrder(hepMatchOrder);
    builder.addRuleCollection(Lists.newArrayList(ruleSet.iterator()));
    return runHepPlanner(builder.build(), input, targetTraits, context);
  }

  /**
   * run HEP planner with rules applied one by one. First apply one rule to all of the nodes
   * and only then apply the next rule. If a rule creates a new node preceding rules will not
   * be applied to the newly created node.
   * */
  static RelNode runHepPlannerSequentially(
      HepMatchOrder hepMatchOrder,
      RuleSet ruleSet,
      RelNode input,
      RelTraitSet targetTraits,
      Context context) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addMatchOrder(hepMatchOrder);
    for (RelOptRule relOptRule : ruleSet) {
      builder.addRuleInstance(relOptRule);
    }
    return runHepPlanner(builder.build(), input, targetTraits, context);
  }



  private static RelNode runHepPlanner(
      HepProgram hepProgram,
      RelNode input,
      RelTraitSet targetTraits,
      Context context) {
    /*
     * 1: HepProgram
     *
     *    HepProgram表示一个或一组优化规则, 这些优化规则作用到每个节点上
     *
     * 2: Context
     *
     *    Context的主要是用来传递信息的, 如外层传入的参数可以在优化规则中获取
     *
     * */
    HepPlanner planner = new HepPlanner(hepProgram, context);
    /*
     * HepPlanner.setRoot()
     *
     * 1: RelNode转为HepRelVertex, 对应DirectedGraph中节点
     *
     * 2: 构建DirectedGraph, HepPlanner基于DirectedGraph按照HepMatchOrder搜索方向搜索符合规则的节点进行优化
     *
     *             SyntaxTree                 ====>>                DirectedGraph
     *
     *            Project(#3)                                 HepRelVertex(Project(#3))
     *                /|\                                                  |
     *                 |                                                   |
     *      +----------+---------+                         +---------------+---------------+
     *      |                    |                         |                               |
     *      |                    |                        \|/                             \|/
     *  TableScan(#1)     TableScan(#2)    HepRelVertex(TableScan(#1))     HepRelVertex(TableScan(#2))
     *
     * */
    planner.setRoot(input);
    if (input.getTraitSet() != targetTraits) {
      planner.changeTraits(input, targetTraits.simplify());
    }
    /*
     * RelOptPlanner.findBestExp()
     *
     * 1: 按照HepMatchOrder搜索方向, 搜索符合规则的节点进行优化
     *
     *    HepMatchOrder分为四类: ARBITRARY、DEPTH_FIRST、BOTTOM_UP、TOP_DOWN
     *
     *    1.1 ARBITRARY、DEPTH_FIRST
     *
     *        深度优先遍历, 比较高效
     *
     *    1.2 BOTTOM_UP、TOP_DOWN
     *
     *        TOP_DOWN: 首先从DirectedGraph入度为零的顶点开始搜索, 即从HepRelVertex(Project(#3))开始
     *
     *        BOTTOM_UP: TOP_DOWN的逆过程
     *
     *        注意:
     *
     *          a: 搜索完成的节点, 会将该节点的孩子节点入度减一, 若孩子节点入度变为零, 则开始优化孩子节点, 以此类推
     *
     *          b: 当符合规则的节点优化完成后, 会重新对DirectedGraph中的所有节点再次搜索, 这就有可能导致死循环, 规则优化器
     *
     *             通过MatchLimit(默认是无穷大)来解决这个问题.
     *
     *             假如优化规则A: left join -> right join, 优化规则B: right join -> left join, 则会导致死循环
     *
     * 2: 优化规节点
     *
     *    2.1 RelOptRule
     *
     *        2.1.1 RelOptRuleOperand
     *
     *           RelOptRuleOperand.matches()决定节点是否符合规则, 判断依据: 类型和节点特征是否符合
     *
     *        2.1.2 RelOptRule.onMatch(RelOptRuleCall calls)
     *
     *           若节点符合优化的规则, 则方法被触发. 若将节点生成新的节点, 则HepPlanner对生成新节点建立节点间依赖关系, 同时删除旧节点
     *
     *    2.2 RelOptRuleCall
     *
     *        RelOptRuleCall包含了节点优化的上下文信息, 节点优化的后结果也记录在该对象中
     *
     * 3: RelNode的唯一标识
     *
     *    在SQL语法树种, 每个RelNode节点的唯一标识可有RelNode.getDigest()和RelNode.getRowType()两者决定
     *
     * */
    return planner.findBestExp();
  }


}
