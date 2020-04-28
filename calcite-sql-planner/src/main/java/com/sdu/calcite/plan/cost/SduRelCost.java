package com.sdu.calcite.plan.cost;

import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;

import org.apache.calcite.plan.RelOptCost;

public class SduRelCost implements RelOptCost {

  /*
   * Volcano Planner基于代价的优化器, RelOptCost标识RelNode节点执行代价
   *
   * Volcano Planner代价评估主要基于数据记录数、数据处理耗CPU、数据读取耗IO等参数衡量
   * */

  // RelNode初始代价
  static final SduRelCost INFINITY = new SduRelCost(POSITIVE_INFINITY, POSITIVE_INFINITY, POSITIVE_INFINITY) {
    @Override
    public String toString() {
      return "{infinity}";
    }
  };

  // Volcano
  static final SduRelCost HUGE = new SduRelCost(MAX_VALUE, MAX_VALUE, MAX_VALUE) {
    @Override
    public String toString() {
      return "{huge}";
    }
  };

  static final SduRelCost ZERO = new SduRelCost(0.0, 0.0, 0.0) {
    @Override
    public String toString() {
      return "{zero}";
    }
  };

  static final SduRelCost TINY = new SduRelCost(1.0, 1.0, 0.0) {
    @Override
    public String toString() {
      return "{tiny}";
    }
  };


  private final double rowCount;
  private final double cpu;
  private final double io;

  SduRelCost(double rowCount, double cpu, double io) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  @Override
  public double getRows() {
    return rowCount;
  }

  @Override
  public double getCpu() {
    return cpu;
  }

  @Override
  public double getIo() {
    return io;
  }

  @Override
  public boolean isInfinite() {
    return this == SduRelCost.INFINITY
        || this.rowCount == POSITIVE_INFINITY
        || this.cpu == POSITIVE_INFINITY
        || this.io == POSITIVE_INFINITY;
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof SduRelCost
        && (this.rowCount == ((SduRelCost) other).rowCount)
        && (this.cpu == ((SduRelCost) other).cpu)
        && (this.io == ((SduRelCost) other).io);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SduRelCost) {
      return equals((SduRelCost) obj);
    }
    return false;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost cost) {
    return false;
  }

  @Override
  public boolean isLe(RelOptCost cost) {
    SduRelCost that = (SduRelCost) cost;
    /*
     * 若是与cost相等或者比cost小, 则返回true
     *
     * 1: 优先比较CPU: 若CPU利用率低, 则认为代价低, 否则认为代价高
     *
     * 2: 其次比较IO: 前提是CPU利用率相同, 否则直接返回false
     *
     * 3: 最后比较数据记录数
     * */
    if (true) {
      return this == that
          || this.rowCount <= that.rowCount;
    }
    return (this == that)
        || ((this.rowCount <= that.rowCount)
        && (this.cpu <= that.cpu)
        && (this.io <= that.io));
  }

  @Override
  public boolean isLt(RelOptCost cost) {
    // 若是比cost小, 则返回true
    if (true) {
      SduRelCost that = (SduRelCost) cost;
      return this.rowCount < that.rowCount;
    }
    return isLe(cost) && !equals(cost);
  }

  @Override
  public RelOptCost plus(RelOptCost cost) {
    if (cost == INFINITY || this == INFINITY) {
      return INFINITY;
    }
    return new SduRelCost(this.rowCount + cost.getRows(),
        this.cpu + cost.getCpu(),
        this.io + cost.getIo());
  }

  @Override
  public RelOptCost minus(RelOptCost cost) {
    if (this == INFINITY) {
      return INFINITY;
    }
    return new SduRelCost(this.rowCount - cost.getRows(),
        this.cpu - cost.getCpu(),
        this.io - cost.getIo());
  }

  @Override
  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new SduRelCost(this.rowCount * factor,
        this.cpu * factor,
        this.io * factor);
  }

  @Override
  public double divideBy(RelOptCost cost) {
    double d = 1.0;
    double n = 0.0;

    if (this.rowCount != 0 && !Double.isInfinite(this.rowCount) &&
        cost.getRows() != 0 && !Double.isInfinite(cost.getRows())) {
      d *= this.rowCount / cost.getRows();
      n += 1;
    }

    if (this.cpu != 0 && !Double.isInfinite(this.cpu) &&
        cost.getCpu() != 0 && !Double.isInfinite(cost.getCpu())) {
      d *= this.cpu / cost.getCpu();
      n += 1;
    }

    if (this.io != 0 && !Double.isInfinite(this.io) &&
        cost.getIo() != 0 && !Double.isInfinite(cost.getIo())) {
      d *= this.io / cost.getIo();
      n += 1;
    }

    if (n == 0) {
      return 1.0;
    }

    return Math.pow(d, 1 / n);
  }

  @Override
  public String toString() {
    return format("{%g rows, %g cpu, %g io}", rowCount, cpu, io);
  }

}
