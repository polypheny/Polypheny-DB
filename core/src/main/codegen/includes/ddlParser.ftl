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

<#-- @formatter:off -->
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


SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
}
{
    <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.end(this), replace, ifNotExists, id);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List
    <SqlNode> list = new ArrayList
    <SqlNode>();
}
{
    <OPTIONS> { s = span(); }
    <LPAREN>
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
    final List
    <SqlNode> list = new ArrayList
    <SqlNode>();
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
    final String collation;
}
{
    id = SimpleIdentifier()
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
            [ <GENERATED> <ALWAYS> ]
            <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY)
            <RPAREN>
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
                    strategy = nullable ? ColumnStrategy.NULLABLE : ColumnStrategy.NOT_NULLABLE;
                }
        )
        (
                <COLLATE>
                (
                    <CASE> <SENSITIVE> { collation = "CASE SENSITIVE"; }
                    |
                    <CASE> <INSENSITIVE> { collation = "CASE INSENSITIVE"; }
                )
            |
                {
                  collation = null;
                }
        )
        {
            list.add( SqlDdlNodes.column(s.add(id).end(this), id, type.withNullable(nullable), collation, e, strategy));
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
            <CHECK> { s.add(this); }
            <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY)
            <RPAREN> {
                list.add(SqlDdlNodes.check(s.end(this), name, e));
            }
            |
                <UNIQUE> { s.add(this); }
                columnList = ParenthesizedSimpleIdentifierList() {
                    list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
                }
            |
                <PRIMARY> { s.add(this); }
                <KEY>
                columnList = ParenthesizedSimpleIdentifierList() {
                    list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
                }
        )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List
    <SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        (
                <NULL> { nullable = true; }
            |
                <NOT> <NULL> { nullable = false; }
            |
                { nullable = true; }
            )
        )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
        type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
        |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
    SqlIdentifier store = null;
    SqlIdentifier partitionColumn = null;
    SqlIdentifier partitionType = null;
    int numPartitions = 0;
    List<SqlIdentifier> partitionNamesList = new ArrayList<SqlIdentifier>();
    SqlIdentifier partitionName = null;
    List< List<SqlNode>> partitionQualifierList = new ArrayList<List<SqlNode>>();
    List<SqlNode> partitionQualifiers = new ArrayList<SqlNode>();
    SqlNode partitionValues = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    [ <ON> <STORE> store = SimpleIdentifier() ]
    [ <PARTITION> <BY>
                       (
                                partitionType = SimpleIdentifier()
                            |
                                <RANGE> { partitionType = new SqlIdentifier( "RANGE", s.end(this) );}
                       )
        <LPAREN> partitionColumn = SimpleIdentifier() <RPAREN>
        [
            (
                    <PARTITIONS> numPartitions = UnsignedIntLiteral()
                |
                    <WITH> <LPAREN>
                            partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                            (
                                <COMMA> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                            )*
                    <RPAREN>
                |
                    <LPAREN>
                        <PARTITION> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                        <VALUES> <LPAREN>
                                partitionValues = Literal() { partitionQualifiers.add(partitionValues); }
                                (
                                    <COMMA> partitionValues = Literal() { partitionQualifiers.add(partitionValues); }
                                )*
                        <RPAREN> {partitionQualifierList.add(partitionQualifiers); partitionQualifiers = new ArrayList<SqlNode>();}
                        (
                                <COMMA> <PARTITION> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                                        <VALUES> <LPAREN>
                                                partitionValues = Literal() { partitionQualifiers.add(partitionValues); }
                                        <RPAREN> {partitionQualifierList.add(partitionQualifiers); partitionQualifiers = new ArrayList<SqlNode>();}
                        )*
                    <RPAREN>

            )

        ]
    ]
    {
        return SqlDdlNodes.createTable(s.end(this), replace, ifNotExists, id, tableElementList, query, store, partitionType, partitionColumn, numPartitions, partitionNamesList, partitionQualifierList);
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, columnList, query);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
            <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
        |
            <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
        |
            <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), replace, ifNotExists, id, className, usingList);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <SCHEMA> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}

