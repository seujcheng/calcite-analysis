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

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
(name = value [, name = value]*)
LPAREN: 左括号
COMMA: 逗号
RPAREN: 右括号
**/
SqlNodeList DefineProperties() :
{
    final Span s = Span.of();
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN>
    {
        s.add(this);
    }
    PropertyDef(list)
    (
        <COMMA> PropertyDef(list)
    )*
    <RPAREN>
    {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
name = value
**/
void PropertyDef(List<SqlNode> list) :
{
    final Span s = Span.of();
    final SqlIdentifier name;
    final SqlNode val;
}
{
    name = SimpleIdentifier() { s.add(this); }
    <EQ>
    val = Literal()
    {
        list.add(new SqlPropertyNode(s.end(val), name, val));
    }
}

void KeyWithProperties(Map<String, SqlNodeList> jobConf) :
{
   final SqlIdentifier name;
   final SqlNodeList properties;
}
{
   name = SimpleIdentifier()
   <WITH>
      properties = DefineProperties()
   {
       jobConf.put(name.toString(), properties);
   }
}

/**
key WITH (name = value [, name = value]*) [, key WITH (name = value [, name = value]*)]*
**/
Map<String, SqlNodeList> KeyWithMultiProperties() :
{
  final Map<String, SqlNodeList> keyProperties = new HashMap<String, SqlNodeList>();
}
{
   KeyWithProperties(keyProperties)
   (
       <COMMA> KeyWithProperties(keyProperties)
   )*
   {
      return keyProperties;
   }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        (
            <NULL> { nullable = true; }
        |
            <NOT> <NULL> { nullable = false; }
        |
            { nullable = true; }
        )
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, strategy));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNodeList tableDescriptor = null;
    SqlNode query = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    [<WITH> tableDescriptor = DefineProperties() ]
    {
        return new SqlCreateTable(s.end(this), replace, ifNotExists,
                id, tableElementList, tableDescriptor, null);
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
    [ <WITH> properties = DefineProperties() ]
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
    [ <WITH> properties = DefineProperties() ]
    {
        return new SqlUseFunction(s.end(this), name, className, properties);
    }
}

/**
DEFINE JOB job_name SET key WITH (name = value [, name = value]*) [, key WITH (name = value [, name = value]*)]
**/
SqlCall SqlJobDefine() :
{
   final Span s = Span.of();
   final SqlIdentifier name;
   Map<String, SqlNodeList> jobConf = null;
}
{
    <DEFINE> <JOB>
        name = SimpleIdentifier()
    [ <SET> jobConf = KeyWithMultiProperties() ]
    {
        return new SqlJobDefine(s.end(this), name, jobConf);
    }
}

// End parserImpls.ftl
