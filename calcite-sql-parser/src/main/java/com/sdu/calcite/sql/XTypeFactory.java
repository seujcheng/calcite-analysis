package com.sdu.calcite.sql;

import java.nio.charset.Charset;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.util.ConversionUtil;

public class XTypeFactory extends JavaTypeFactoryImpl {

  public XTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  @Override
  public Charset getDefaultCharset() {
    // SQL中文处理
    return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
  }

}
