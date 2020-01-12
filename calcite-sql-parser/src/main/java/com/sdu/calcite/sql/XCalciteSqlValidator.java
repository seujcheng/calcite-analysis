package com.sdu.calcite.sql;

import com.sdu.calcite.sql.table.XNodePath;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

public class XCalciteSqlValidator extends SqlValidatorImpl {

  public XCalciteSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, SqlConformance conformance) {
    super(opTab, catalogReader, typeFactory, conformance);
  }


  public Set<XNodePath> getAggregateNodePaths(SqlSelect select) {
    Set<XNodePath> nodePaths = new HashSet<>();
    if (select == null) {
      return nodePaths;
    }

    if (select.getGroup() == null) {
      // FROM
      SqlNode from = select.getFrom();
      if (from.getKind() == SqlKind.SELECT) {
        Set<XNodePath> subNodePaths = getAggregateNodePaths((SqlSelect) from);
        if (subNodePaths != null && !subNodePaths.isEmpty()) {
          nodePaths.addAll(subNodePaths);
        }
        return nodePaths;
      }
    }

    SqlNodeList groupNode = select.getGroup();

    // TODO: 2020-01-10
    return nodePaths;
  }

}
