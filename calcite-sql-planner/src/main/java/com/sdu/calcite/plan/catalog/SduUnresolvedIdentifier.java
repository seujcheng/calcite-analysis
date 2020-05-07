package com.sdu.calcite.plan.catalog;

import com.google.common.base.Preconditions;
import com.sdu.calcite.api.SduValidationException;
import java.util.Arrays;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public final class SduUnresolvedIdentifier {

  private final String catalogName;
  private final String databaseName;
  private final String objectName;

  private SduUnresolvedIdentifier(String catalogName, String databaseName, String objectName) {
    this.catalogName = catalogName;
    this.databaseName = databaseName;
    this.objectName = Preconditions.checkNotNull(objectName, "Object name must not be null.");;
  }

  public Optional<String> getCatalogName() {
    return Optional.ofNullable(catalogName);
  }

  public Optional<String> getDatabaseName() {
    return Optional.ofNullable(databaseName);
  }

  public String getObjectName() {
    return objectName;
  }

  public static SduUnresolvedIdentifier of(String... path) {
    if (path == null) {
      throw new SduValidationException("Object identifier can not be null!");
    }
    if (path.length < 1 || path.length > 3) {
      throw new SduValidationException("Object identifier must consist of 1 to 3 parts.");
    }
    if (Arrays.stream(path).anyMatch(StringUtils::isEmpty)) {
      throw new SduValidationException("Parts of the object identifier are null or whitespace-only.");
    }

    if (path.length == 3) {
      return new SduUnresolvedIdentifier(path[0], path[1], path[2]);
    } else if (path.length == 2) {
      return new SduUnresolvedIdentifier(null, path[0], path[1]);
    } else {
      return new SduUnresolvedIdentifier(null, null, path[0]);
    }
  }

}
