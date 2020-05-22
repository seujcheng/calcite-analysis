package com.sdu.calcite.sql.utils;

import com.sdu.calcite.sql.parser.ParseException;
import org.apache.calcite.runtime.Resources;

public interface ParserResource {

  ParserResource RESOURCE = Resources.create(ParserResource.class);

  @Resources.BaseMessage("Multiple WATERMARK statements is not supported yet.")
  Resources.ExInst<ParseException> multipleWatermarksUnsupported();

}
