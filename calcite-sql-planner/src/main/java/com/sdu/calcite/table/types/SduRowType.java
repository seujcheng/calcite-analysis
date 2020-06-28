package com.sdu.calcite.table.types;

import com.google.common.base.Preconditions;
import com.sdu.calcite.api.SduValidationException;
import com.sdu.calcite.table.data.SduRowData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

public class SduRowType extends SduLogicalType {

  private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
      SduRowData.class.getName());

  private List<RowField> fields;

  private SduRowType(List<RowField> fields) {
    super(true, SduLogicalTypeRoot.ROW);
    this.fields = Collections.unmodifiableList(
        new ArrayList<>(
            Preconditions.checkNotNull(fields, "Fields must not be null.")));
    validateFields(fields);
  }

  public SduRowType(boolean isNullable, List<RowField> fields) {
    super(isNullable, SduLogicalTypeRoot.ROW);
    this.fields = Collections.unmodifiableList(
        new ArrayList<>(
            Preconditions.checkNotNull(fields, "Fields must not be null.")));

    validateFields(fields);
  }

  public int getFieldCount() {
    throw new RuntimeException("");
  }

  @Override
  public List<SduLogicalType> getChildren() {
    return Collections.unmodifiableList(
        fields.stream()
            .map(RowField::getType)
            .collect(Collectors.toList())
    );
  }

  @Override
  public boolean supportsInputConversion(Class<?> clazz) {
    return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public boolean supportsOutputConversion(Class<?> clazz) {
    return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
  }

  @Override
  public <R> R accept(SduLogicalTypeVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public static final class RowField implements Serializable {

    private final String name;

    private final SduLogicalType type;

    private final @Nullable String description;

    public RowField(String name, SduLogicalType type, @Nullable String description) {
      this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
      this.type = Preconditions.checkNotNull(type, "Field type must not be null.");
      this.description = description;
    }

    public String getName() {
      return name;
    }

    public SduLogicalType getType() {
      return type;
    }

    public Optional<String> getDescription() {
      return Optional.ofNullable(description);
    }

  }

  public static SduRowType of(SduLogicalType[] types, String[] names) {
    List<RowField> fields = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      fields.add(new RowField(names[i], types[i], null));
    }
    return new SduRowType(fields);
  }

  private static void validateFields(List<RowField> fields) {
    final List<String> fieldNames = fields.stream()
        .map(f -> f.name)
        .collect(Collectors.toList());
    if (fieldNames.stream().anyMatch(StringUtils::isEmpty)) {
      throw new SduValidationException("Field names must contain at least one non-whitespace character.");
    }
    final Set<String> duplicates = fieldNames.stream()
        .filter(n -> Collections.frequency(fieldNames, n) > 1)
        .collect(Collectors.toSet());
    if (!duplicates.isEmpty()) {
      throw new SduValidationException(
          String.format("Field names must be unique. Found duplicates: %s", duplicates));
    }
  }

}
