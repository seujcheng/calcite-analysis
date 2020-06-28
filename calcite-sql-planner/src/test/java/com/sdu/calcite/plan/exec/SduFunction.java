package com.sdu.calcite.plan.exec;

import com.sdu.calcite.table.data.SduRowData;
import java.util.Map;

public interface SduFunction {

  void open(Map<String, String> conf) throws Exception;

  SduRowData process(SduRowData input);

  void close() throws Exception;

}
