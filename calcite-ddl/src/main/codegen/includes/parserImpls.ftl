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
(name = value [, name = value]*)
LPAREN: 左括号
COMMA: 逗号
RPAREN: 右括号
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
name = value
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

void TableColumn(List<SqlNode> list) :
{
   SqlParserPos pos;
   SqlIdentifier name;
   SqlDataTypeSpec type;
   SqlCharStringLiteral comment = null;
}
{
    name = SimpleIdentifier()
    type = DataType()
    [ <COMMENT> <QUOTED_STRING> {
        String p = SqlParserUtil.parseString(token.image);
        comment = SqlLiteral.createCharString(p, getPos());
    }]
    {
        SqlTableColumn column = new SqlTableColumn(getPos(), name, type, comment);
        list.add(column);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SqlParserPos startPos = s.pos();
    SqlIdentifier tableName;
    SqlNodeList columnList = SqlNodeList.EMPTY;
    SqlNodeList tableProps = SqlNodeList.EMPTY;
    SqlCharStringLiteral comment = null;

    SqlParserPos pos = startPos;
}
{
    <TABLE>

    // 解析表名
    tableName = CompoundIdentifier()

    // 解析列
    [
        <LPAREN>
            { pos = getPos(); List<SqlNode> columns = new ArrayList<SqlNode>();}
            TableColumn(columns)
            (
                <COMMA> TableColumn(columns)
            )*
        <RPAREN>
        {
            pos = pos.plus(getPos());
            columnList = new SqlNodeList(columns, pos);
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
        tableProps = SqlOptions()
    ]

    {
        return new SqlCreateTable(startPos.plus(getPos()), tableName, columnList, tableProps, comment);
    }
}

/**
CREATE FUNCTION function_name AS class_name [WITH ( name = value [, name = value]* )]
**/
SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final SqlIdentifier name;
    final SqlNode className;
    SqlNodeList properties = null;
}
{
    <FUNCTION>
        name = SimpleIdentifier()
    <AS>
        className = StringLiteral()
    [ <WITH> properties = SqlOptions() ]
    {
        return new SqlCreateFunction(s.end(this), name, className, properties);
    }
}

/**
USE FUNCTION function_name AS class_name WITH (name = value [, name = value]*)
**/
SqlCall SqlUseFunction() :
{
   final Span s = Span.of();
   final SqlIdentifier name;
   final SqlNode className;
   SqlNodeList properties = null;
}
{
    <USE> <FUNCTION>
        name = SimpleIdentifier()
    <AS>
        className = StringLiteral()
    [ <WITH> properties = SqlOptions() ]
    {
        return new SqlUseFunction(s.end(this), name, className, properties);
    }
}

// End parserImpls.ftl
