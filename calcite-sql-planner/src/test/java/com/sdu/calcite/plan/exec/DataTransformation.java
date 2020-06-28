package com.sdu.calcite.plan.exec;

import com.sdu.calcite.table.data.SduRowData;
import com.sdu.calcite.table.types.SduRowType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class DataTransformation {

  private final DataTransformation input;

  private final SduFunction function;

  DataTransformation() {
    this.input = null;
    this.function = null;
  }

  DataTransformation(DataTransformation input, SduFunction function) {
    this.input = Objects.requireNonNull(input);
    this.function = Objects.requireNonNull(function);
  }

  public Optional<DataTransformation> getInput() {
    return Optional.ofNullable(input);
  }

  public Optional<SduFunction> getFunction() {
    return Optional.ofNullable(function);
  }


  public abstract List<SduRowData> getOutput();

  public abstract SduRowType getOutputType();

}
