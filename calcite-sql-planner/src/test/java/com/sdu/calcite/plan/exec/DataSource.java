package com.sdu.calcite.plan.exec;

import com.google.common.base.Preconditions;
import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduRowType;
import java.util.List;

public class DataSource extends DataTransformation {

  private final List<SduRowData> inputData;
  private final SduRowType inputType;

  public DataSource(SduRowType inputType, List<SduRowData> inputData) {
    this.inputType = Preconditions.checkNotNull(inputType);
    this.inputData = Preconditions.checkNotNull(inputData);
  }

  @Override
  public List<SduRowData> getOutput() {
    return inputData;
  }

  @Override
  public SduRowType getOutputType() {
    return inputType;
  }


}
