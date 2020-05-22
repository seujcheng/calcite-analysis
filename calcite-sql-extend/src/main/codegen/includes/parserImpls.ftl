<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

/**
   * (name = value [, name = value]*)
   * LPAREN: 左括号
   * COMMA: 逗号
   * RPAREN: 右括号
   **/
SqlNodeList SqlOptions() :
{
    SqlNode property;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final Span span;
}
{
    <LPAREN>
    {
        span = span();
    }
    [
        property = SqlOption()
        {
            list.add(property);
        }
        (
            <COMMA> property = SqlOption()
            {
                list.add(property);
            }
        )*
    ]
    <RPAREN>
    {
        return new SqlNodeList(list, span.end(this));
    }
}

/**
  *  name = value
  **/
SqlNode SqlOption() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = StringLiteral()
    { pos = getPos(); }
    <EQ>
    value = StringLiteral()
    {
        return new SqlOption(getPos(), key, value);
    }
}

/**
  * 表列有两种格式:
  *
  * 1: 常量列, 其语法格式: column type [COMMENT]
  *
  * 2: 计算列, 其语法格式: column AS compute_expression [COMMENT]
  *
  * 3: Watermark, 其语法格式: WATERMARK FOR column AS watermark_expression
  *
  * JavaCC解析列时, 需要看两个Token便可区分(即第二个Token是否为AS)
  */
void TableColumn(TableCreationContext context):
{
}
{
   (LOOKAHEAD(2)
      TableColumn2(context.columns)
   |
      ComputeColumn(context.columns)
   |
      Watermark(context)
   )
}

void TableColumn2(List<SqlNode> list) :
{
   SqlParserPos pos;
   SqlIdentifier name;
   SqlDataTypeSpec type;
   SqlCharStringLiteral path = null;
   SqlCharStringLiteral comment = null;
}
{
    name = SimpleIdentifier()
    type = DataType()
    // QUOTED_STRING: 引号
    [
        <QUOTED_STRING> {
            String p = SqlParserUtil.parseString(token.image);
            path = SqlLiteral.createCharString(p, getPos());
        }
    ]
    [
        <COMMENT> <QUOTED_STRING> {
            String p = SqlParserUtil.parseString(token.image);
            comment = SqlLiteral.createCharString(p, getPos());
        }
    ]

    {
        SqlTableColumn column = new SqlTableColumn(getPos(), name, type, path, comment);
        list.add(column);
    }
}

void ComputeColumn(List<SqlNode> list):
{
    SqlNode identifier;
    SqlNode expr;
    SqlParserPos pos;
}
{
    identifier = SimpleIdentifier() {pos = getPos();}
    <AS>
    expr = Expression(ExprContext.ACCEPT_NON_QUERY)
    {
        expr = SqlStdOperatorTable.AS.createCall(Span.of(identifier, expr).pos(), expr, identifier);
        list.add(expr);
    }
}

void Watermark(TableCreationContext context):
{
   SqlIdentifier  eventTimeColumn;
   SqlParserPos pos;
   SqlNode strategy;
}
{
    <WATERMARK> {pos = getPos();} <FOR>
    eventTimeColumn = CompoundIdentifier()
    <AS>
    strategy = Expression(ExprContext.ACCEPT_NON_QUERY)
    {
        if (context.watermark != null) {
          throw SqlUtil.newContextException(pos, ParserResource.RESOURCE.multipleWatermarksUnsupported());
        }
        context.watermark = new SqlWatermark(pos, eventTimeColumn, strategy);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    SqlIdentifier name;
    SqlNodeList tableColumns = SqlNodeList.EMPTY;
    SqlWatermark watermark = null;
    SqlNodeList properties = SqlNodeList.EMPTY;
    SqlCharStringLiteral comment = null;

    SqlParserPos pos = startPos;
}
{
    <TABLE>

    // 解析表名
    name = CompoundIdentifier()

    // 解析列
    [
        <LPAREN>
            {
                pos = getPos();
                TableCreationContext context = new TableCreationContext();
            }
            TableColumn(context)
            (
                <COMMA> TableColumn(context)
            )*
        <RPAREN>
        {
            pos = pos.plus(getPos());
            tableColumns = new SqlNodeList(context.columns, pos);
            watermark = context.watermark;
        }
    ]

    // 解析表注释
    [
        <COMMENT> <QUOTED_STRING> {
            String p = SqlParserUtil.parseString(token.image);
            comment = SqlLiteral.createCharString(p, getPos());
        }
    ]

    // 解析表属性
    [
        <WITH>
        properties = SqlOptions()
    ]

    {
        return new SqlCreateTable(startPos.plus(getPos()), name, tableColumns, watermark,  comment, properties);
    }
}

/**
  * CREATE FUNCTION func_name [WITH ( name = value [, name = value]* )]
  **/
SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final SqlIdentifier name;
    SqlNodeList properties = SqlNodeList.EMPTY;
}
{
    <FUNCTION>

        name = SimpleIdentifier()

    [
        <WITH> properties = SqlOptions()
    ]

    {
        return new SqlCreateFunction(s.end(this), name, properties);
    }
}

// End parserImpls.ftl
