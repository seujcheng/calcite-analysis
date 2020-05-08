package org.apache.calcite.sql.validate;

import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

public class ParameterScope extends EmptyScope {

  //~ Instance fields --------------------------------------------------------

  /**
   * Map from the simple names of the parameters to types of the parameters
   * ({@link RelDataType}).
   */
  private final Map<String, RelDataType> nameToTypeMap;

  //~ Constructors -----------------------------------------------------------

  ParameterScope(
      SqlValidatorImpl validator,
      Map<String, RelDataType> nameToTypeMap) {
    super(validator);
    this.nameToTypeMap = nameToTypeMap;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlQualified fullyQualify(SqlIdentifier identifier) {
    return SqlQualified.create(this, 1, null, identifier);
  }

  public SqlValidatorScope getOperandScope(SqlCall call) {
    return this;
  }

  @Override
  public RelDataType resolveColumn(String name, SqlNode ctx) {
    //  默认返回NULL, 则无法找到参与计算列的物理列数据类型
    return nameToTypeMap.get(name);
  }
}
