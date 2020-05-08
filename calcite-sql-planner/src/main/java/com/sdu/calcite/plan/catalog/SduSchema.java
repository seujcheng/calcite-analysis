package com.sdu.calcite.plan.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;

/*
 * 1: Schema描述元数据, 元数据可以"树"结构管理, 如:不同机房(A)部署不同的数据库集群(B), 不同的数据库集群(B)存放不同的数据库表(C), 其元
 *
 *    数据的层次结果如下:
 *
 *    +-----+         +-----+         +-----+
 *    |  A  |  <----  |  B  |  <----  |  C  |
 *    +-----+         +-----+         +-----+
 *
 *    三种数据(Schema)需实现方法: getSubSchema(name)
 *
 *    方法返回孩子节点元数据, 如: 机房元数据的该方法返回给定机房下部署的数据库集群元数据
 *
 * 2: 三种数据(Schema)选择实现: getTable(name)
 *
 *    只有数据库存放数据库表, 故数据库表元数据(C)必须实现该方法, 其他两种元数据无需实现
 *
 * 3: 在Calcite中, 元数据查询具体可查看org.apache.calcite.sql.validate.EmptyScope.resolve_()方法
 *
 *    Calcite对SqlValidatorScope以"树"方式管理, EmptyScope是根节点
 *
 * */
public abstract class SduSchema implements Schema {

  @Override
  public RelProtoDataType getType(String name) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return Collections.emptySet();
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames() {
    return Collections.emptySet();
  }

  @Override
  public Schema snapshot(SchemaVersion version) {
    return this;
  }

}
