package com.sdu.calcite.plan.exec;

import com.google.common.base.Preconditions;
import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduRowType;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class GeneralDataTransformation extends DataTransformation {

  private final SduRowType outputType;

  public GeneralDataTransformation(DataTransformation input, SduFunction function, SduRowType outputType) {
    super(input, function);
    this.outputType = Preconditions.checkNotNull(outputType);
  }

  @Override
  public List<SduRowData> getOutput() {
    List<SduRowData> inputData = getInput()
        .orElseThrow(() -> new IllegalStateException("Data Input Null ....."))
        .getOutput();
    if (inputData == null || inputData.isEmpty()) {
      return Collections.emptyList();
    }

    SduFunction function = getFunction()
        .orElseThrow(() -> new IllegalStateException("Data Processor SduFunction Null"));

    return inputData.stream()
        .map(function::process)
        .collect(Collectors.toList());
  }

  @Override
  public SduRowType getOutputType() {
    return outputType;
  }

}
