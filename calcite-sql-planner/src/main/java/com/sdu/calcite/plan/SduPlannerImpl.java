package com.sdu.calcite.plan;

import static com.sdu.calcite.CalciteSchemaBuilder.asRootSchema;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.sdu.calcite.SduParser;
import com.sdu.calcite.SduParserImpl;
import com.sdu.calcite.api.SduCatalogManager;
import com.sdu.calcite.api.SduTableConfig;
import com.sdu.calcite.api.SduTableException;
import com.sdu.calcite.plan.catalog.SduCatalogFunction;
import com.sdu.calcite.plan.catalog.SduCatalogFunctionImpl;
import com.sdu.calcite.plan.catalog.SduCatalogManagerSchema;
import com.sdu.calcite.plan.catalog.SduCatalogTableColumn;
import com.sdu.calcite.plan.catalog.SduCatalogTableColumnImpl;
import com.sdu.calcite.plan.catalog.SduCatalogTableImpl;
import com.sdu.calcite.plan.catalog.SduCatalogTableWatermark;
import com.sdu.calcite.plan.catalog.SduCatalogTableWatermarkImpl;
import com.sdu.calcite.plan.catalog.SduFunctionCatalog;
import com.sdu.calcite.plan.catalog.SduObjectIdentifier;
import com.sdu.calcite.plan.catalog.SduUnresolvedIdentifier;
import com.sdu.calcite.sql.ddl.SqlCreateFunction;
import com.sdu.calcite.sql.ddl.SqlCreateTable;
import com.sdu.calcite.sql.ddl.SqlOption;
import com.sdu.calcite.sql.ddl.SqlTableColumn;
import com.sdu.sql.entry.SduFunction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;

public class SduPlannerImpl implements SduPlanner {

  private final SduTableConfig tableConfig;
  private final SduFunctionCatalog functionCatalog;
  private final SduCatalogManager catalogManager;
  private final SduPlannerContext plannerContext;

  private final Map<String, SduSqlPlanner> sqlPlanners;

  public SduPlannerImpl(SduTableConfig config, SduFunctionCatalog functionCatalog, SduCatalogManager catalogManager) {
    this.tableConfig = requireNonNull(config);
    this.functionCatalog = requireNonNull(functionCatalog);
    this.catalogManager = requireNonNull(catalogManager);
    this.plannerContext = new SduPlannerContext(
        tableConfig,
        this.functionCatalog,
        asRootSchema(new SduCatalogManagerSchema(catalogManager))
    );

    this.sqlPlanners = new HashMap<>();
  }

  @Override
  public SduParser getParser() {
    return new SduParserImpl(plannerContext);
  }

  @Override
  public SqlNode validate(SqlNode sqlNode) {
    SqlNode validated = getSqlPlanner().validate(sqlNode);

    if (validated instanceof SqlCreateTable) {
      SqlCreateTable createTable = (SqlCreateTable) sqlNode;
      createTable(createTable);
    } else if (validated instanceof SqlCreateFunction) {
      SqlCreateFunction createFunction = (SqlCreateFunction) sqlNode;
      createFunction(createFunction);
    }

    return validated;
  }

  @Override
  public RelNode toRel(SqlNode validated) {
    RelRoot relational = getSqlPlanner().rel(validated);
    return relational.rel;
  }

  @Override
  public RelNode optimize(RelNode relNode) {
    Optional<SduRelOptimizerFactory> optimizerFactory = tableConfig.getOptimizerFactory();
    if (optimizerFactory.isPresent()) {
      RelOptPlanner planner = relNode.getCluster().getPlanner();
      Context context = planner.getContext();
      SduRelOptimizer optimizer = optimizerFactory.get().createOptimizer(context, planner);
      return optimizer.optimize(relNode);
    }
    return relNode;
  }

  private SduSqlPlanner getSqlPlanner() {
    String currentCatalog = catalogManager.getCurrentCatalog();
    String currentDatabase = catalogManager.getCurrentDatabaseName();

    String identifier = format("%s.%s", currentCatalog, currentDatabase);
    return sqlPlanners.computeIfAbsent(identifier, key -> plannerContext.createPlanner(currentCatalog, currentDatabase));
  }

  private String getQuotedSqlString(SqlNode sqlNode) {
    SqlParser.Config parserConfig = getSqlPlanner().config().getParserConfig();
    SqlDialect dialect = new CalciteSqlDialect(SqlDialect.EMPTY_CONTEXT
        .withQuotedCasing(parserConfig.unquotedCasing())
        .withConformance(parserConfig.conformance())
        .withUnquotedCasing(parserConfig.unquotedCasing())
        .withIdentifierQuoteString(parserConfig.quoting().string));
    return sqlNode.toSqlString(dialect).getSql();
  }

  private void createTable(SqlCreateTable createTable) {
    final SduSqlValidator validator = getSqlPlanner().getOrCreateSqlValidator();

    // 列
    final Map<String, RelDataType> physicalColumnNameToTypes = new HashMap<>();
    final Map<String, RelDataType> computedColumnNameToTypes = new HashMap<>();
    createTable.getColumns()
        .getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlTableColumn)
        .forEach(sqlNode -> {
          SqlTableColumn column = (SqlTableColumn) sqlNode;
          RelDataType relType = column.getType().deriveType(validator);
          String name = column.getName().getSimple();
          physicalColumnNameToTypes.put(name, relType);
        });

    List<SduCatalogTableColumn> columns = new ArrayList<>();
    for (SqlNode sqlNode : createTable.getColumns()) {
      if (sqlNode instanceof SqlTableColumn) {
        // 物理列
        SqlTableColumn column = (SqlTableColumn) sqlNode;
        String name = column.getName().getSimple();
        String comment = column.getComment()
            .map(stringLiteral -> stringLiteral.getNlsString().getValue())
            .orElse("");
        columns.add(new SduCatalogTableColumnImpl(
            name,
            physicalColumnNameToTypes.get(name).getSqlTypeName().getName(),
            null,
            comment
        ));
      } else if (sqlNode instanceof SqlBasicCall) {
        // 虚拟列(计算列), 定义格式: column AS expr, 解析格式: expr AS column
        SqlBasicCall call = (SqlBasicCall) sqlNode;
        SqlNode validatedExpr = validator.validateParameterizedExpression(call.operand(0), physicalColumnNameToTypes);
        RelDataType validatedType = validator.getValidatedNodeType(validatedExpr);
        String computedColumnName = call.operand(1).toString();
        computedColumnNameToTypes.put(computedColumnName, validatedType);
        columns.add(new SduCatalogTableColumnImpl(
            computedColumnName,
            validatedType.getSqlTypeName().getName(),
            getQuotedSqlString(validatedExpr),
            null
        ));
      } else {
        throw new SduTableException("Unexpected table column type!");
      }
    }

    // 水印列
    SduCatalogTableWatermark tableWatermark = createTable.getWatermark()
        .map(watermark -> {
          String rowtimeColumn = watermark.getEventTimeColumn().toString();

          Map<String, RelDataType> columnNameToTypes = new HashMap<>(physicalColumnNameToTypes);
          columnNameToTypes.putAll(computedColumnNameToTypes);

          // 校验语法是否合法
          SqlNode expression = watermark.getStrategy();
          SqlNode validated = validator.validateParameterizedExpression(expression, columnNameToTypes);
          RelDataType validatedType = validator.getValidatedNodeType(validated);

          return new SduCatalogTableWatermarkImpl(
              rowtimeColumn,
              getQuotedSqlString(validated),
              validatedType.getSqlTypeName().getName()
          );

        }).orElse(null);

    // 表属性
    Map<String, String> tableProps = new HashMap<>();
    createTable.getProperties()
        .ifPresent(sqlNodes ->
            sqlNodes.getList()
                .stream()
                .filter(sqlNode -> sqlNode instanceof SqlOption)
                .forEach(sqlNode -> {
                  SqlOption option = (SqlOption) sqlNode;
                  tableProps.put(option.getKeyString(), option.getValueString());
                })
        );

    String comment = createTable.getComment()
        .map(stringLiteral -> stringLiteral.getNlsString().getValue())
        .orElse("");


    SduCatalogTableImpl catalogTable = new SduCatalogTableImpl(
        columns,
        tableWatermark,
        comment,
        tableProps
    );

    SduUnresolvedIdentifier unresolvedIdentifier = SduUnresolvedIdentifier.of(createTable.fullTableName());
    SduObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

    // 注册
    catalogManager.createTable(catalogTable, objectIdentifier, createTable.isIfNotExists());
  }

  private void createFunction(SqlCreateFunction createFunction) {
    SduUnresolvedIdentifier unresolvedIdentifier = SduUnresolvedIdentifier.of(createFunction.getFunctionIdentifier());
    // 自定义函数属性
    Map<String, String> functionProps = new HashMap<>();
    createFunction.getProperties()
        .getList()
        .stream()
        .filter(sqlNode -> sqlNode instanceof SqlOption)
        .forEach(sqlNode -> {
          SqlOption sqlOption = (SqlOption) sqlNode;
          functionProps.put(sqlOption.getKeyString(), sqlOption.getValueString());
        });

    SduObjectIdentifier objectIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
    SduCatalogFunction catalogFunction = new SduCatalogFunctionImpl(functionProps);

    // 注册
    if (catalogFunction.isTemporary()) {
      // TODO:
      SduFunction function = SduFunction.fromSqlCreateFunction(createFunction);
      functionCatalog.registerFunction(createFunction.getFunctionIdentifier()[0], function);
    } else {
      catalogManager.createCatalogFunction(catalogFunction, objectIdentifier, createFunction.isIfNotExists());
    }

  }

}
