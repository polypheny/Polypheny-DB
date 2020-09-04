<#--
Add implementations of additional parser statements, literals or data types.

Example of SqlShowTables() implementation:
SqlNode SqlShowTables()
{
...local variables...
}
{
<SHOW> <TABLES>
...
{
return SqlShowTables(...)
}
}
-->

<#-- @formatter:off -->


/**
* Parses a TRUNCATE TABLE statement.
*/
SqlTruncate SqlTruncateTable() :
{
    final Span s;
    final SqlIdentifier table;
}
{
    <TRUNCATE> { s = span(); }
    <TABLE> table = CompoundIdentifier()
    {
        return new SqlTruncate(s.end(this), table);
    }
}


/**
* Parses a ALTER SCHEMA statement.
*/
SqlAlterSchema SqlAlterSchema(Span s) :
{
    final SqlIdentifier schema;
    final SqlIdentifier name;
    final SqlIdentifier owner;
}
{
    <SCHEMA>
    schema = CompoundIdentifier()
    (
        <RENAME> <TO>
        name = CompoundIdentifier()
        {
            return new SqlAlterSchemaRename(s.end(this), schema, name);
        }
    |
        <OWNER> <TO>
        owner = SimpleIdentifier()
        {
            return new SqlAlterSchemaOwner(s.end(this), schema, owner);
        }
    )
}


/**
* Parses a ALTER TABLE statement.
*/
SqlAlterTable SqlAlterTable(Span s) :
{
    final SqlIdentifier table;
    final SqlIdentifier column;
    final SqlIdentifier name;
    final SqlIdentifier owner;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode defaultValue;
    final SqlIdentifier beforeColumn;
    final SqlIdentifier afterColumn;
    final SqlAlterTable statement;
    final SqlNodeList columnList;
    final SqlNodeList referencesList;
    final SqlIdentifier refColumn;
    final SqlIdentifier refTable;
    final SqlIdentifier constraintName;
    final SqlIdentifier store;
    final SqlIdentifier indexName;
    final SqlIdentifier indexType;
    final String onUpdate;
    final String onDelete;
    final boolean unique;
    final SqlIdentifier partitionType;
    final SqlIdentifier partitionColumn;
    List<Integer> partitionList = new ArrayList<Integer>();
    int partitionIndex = 0;
    int numPartitions = 0;

}
{
    <TABLE>
    table = CompoundIdentifier()
    (
        <RENAME> <TO>
        name = SimpleIdentifier()
        {
            return new SqlAlterTableRename(s.end(this), table, name);
        }
    |
        <OWNER> <TO>
        owner = SimpleIdentifier()
        {
            return new SqlAlterTableOwner(s.end(this), table, owner);
        }
    |
        <RENAME> <COLUMN>
        column = SimpleIdentifier()
        <TO>
        name = SimpleIdentifier()
        {
            return new SqlAlterTableRenameColumn(s.end(this), table, column, name);
        }
    |
        <ADD> <COLUMN>
        name = SimpleIdentifier()
        type = DataType()
        (
            <NULL> { nullable = true; }
        |
            <NOT> <NULL> { nullable = false; }
        |
            { nullable = true; }
        )
        (
            <DEFAULT_>
            defaultValue = Literal()
        |
            { defaultValue = null; }
        )
        (
            <BEFORE> { beforeColumn = SimpleIdentifier(); afterColumn = null; }
        |
            <AFTER> { afterColumn = SimpleIdentifier(); beforeColumn = null; }
        |
            { afterColumn = null; beforeColumn = null; }
        )
        {
            return new SqlAlterTableAddColumn(s.end(this), table, name, type, nullable, defaultValue, beforeColumn, afterColumn);
        }
    |
        <DROP> <COLUMN>
        column = SimpleIdentifier()
        {
            return new SqlAlterTableDropColumn(s.end(this), table, column);
        }
    |
        <ADD> <PRIMARY> <KEY>
        (
            columnList = ParenthesizedSimpleIdentifierList()
        |
            column = SimpleIdentifier()
            {
                columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
            }
        )
        {
            return new SqlAlterTableAddPrimaryKey(s.end(this), table, columnList);
        }
    |
        <DROP> <PRIMARY> <KEY>
        {
            return new SqlAlterTableDropPrimaryKey(s.end(this), table);
        }
    |
        <ADD> <CONSTRAINT>
        constraintName = SimpleIdentifier()
        (
            <UNIQUE>
            (
                columnList = ParenthesizedSimpleIdentifierList()
            |
                column = SimpleIdentifier()
                {
                    columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
                }
            )
            {
                return new SqlAlterTableAddUniqueConstraint(s.end(this), table, constraintName, columnList);
            }
        |
            <FOREIGN> <KEY>
            (
                columnList = ParenthesizedSimpleIdentifierList()
            |
                column = SimpleIdentifier()
                {
                    columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
                }
            )
            <REFERENCES>
            refTable = CompoundIdentifier()
            referencesList = ParenthesizedSimpleIdentifierList()
            (
                <ON> <UPDATE>
                (
                    <CASCADE> { onUpdate = "CASCADE"; }
                |
                    <RESTRICT> { onUpdate = "RESTRICT"; }
                |
                    <SET> <NULL> { onUpdate = "SET NULL"; }
                |
                    <SET> <DEFAULT_> { onUpdate = "SET DEFAULT"; }
                )
            |
                { onUpdate = null; }
            )
            (
                <ON> <DELETE>
                (
                    <CASCADE> { onDelete = "CASCADE"; }
                |
                    <RESTRICT> { onDelete = "RESTRICT"; }
                |
                    <SET> <NULL> { onDelete = "SET NULL"; }
                |
                    <SET> <DEFAULT_> { onDelete = "SET DEFAULT"; }
                )
            |
                { onDelete = null; }
            )
            {
                return new SqlAlterTableAddForeignKey(s.end(this), table, constraintName, columnList, refTable, referencesList, onUpdate, onDelete);
            }
        )
    |
        <DROP> <CONSTRAINT>
        constraintName = SimpleIdentifier()
        {
            return new SqlAlterTableDropConstraint(s.end(this), table, constraintName);
        }
    |
        <DROP>
        <FOREIGN>
        <KEY>
        constraintName = SimpleIdentifier()
        {
            return new SqlAlterTableDropForeignKey(s.end(this), table, constraintName);
        }
    |
        <ADD>
        <PLACEMENT>
        (
            (
                columnList = ParenthesizedSimpleIdentifierList()
            |
                column = SimpleIdentifier()
                {
                    columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
                }
            )
        |
            {
                columnList = SqlNodeList.EMPTY;
            }
        )
        <ON>
        <STORE>
        store = SimpleIdentifier()
            [
                <WITH> <PARTITIONS>
                    <LPAREN>
                    partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    (
                        <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    )*
                    <RPAREN>
            ]
        {
            return new SqlAlterTableAddPlacement(s.end(this), table, columnList, store, partitionList);
        }
    |
        <DROP>
        <PLACEMENT>
        <ON>
        <STORE>
        store = SimpleIdentifier()
        {
            return new SqlAlterTableDropPlacement(s.end(this), table, store);
        }
    |
        <MODIFY>
        <PLACEMENT>
        (
            <ADD>
            <COLUMN>
            column = SimpleIdentifier()
            <ON>
            <STORE>
            store = SimpleIdentifier()
            {
                return new SqlAlterTableModifyPlacementAddColumn(s.end(this), table, column, store);
            }
        |
            <DROP>
            <COLUMN>
            column = SimpleIdentifier()
            <ON>
            <STORE>
            store = SimpleIdentifier()
            {
                return new SqlAlterTableModifyPlacementDropColumn(s.end(this), table, column, store);
            }
        |
            columnList = ParenthesizedSimpleIdentifierList()
            <ON>
            <STORE>
            store = SimpleIdentifier()
            [
                <WITH> <PARTITIONS>
                <LPAREN>
                    partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    (
                        <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    )*
                <RPAREN>
            ]
            {
                return new SqlAlterTableModifyPlacement(s.end(this), table, columnList, store, partitionList);
            }
        )

    |
        <MODIFY>
        <PARTITIONS>
        <LPAREN>
            partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
            (
                <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
            )*
        <RPAREN>
        <ON>
        <STORE> store = SimpleIdentifier()
        {
            return new SqlAlterTableModifyPartitions(s.end(this), table, store, partitionList);
        }

    |
        <ADD>
        (
            <UNIQUE> { unique = true; }
        |
            { unique = false; }
        )
        <INDEX>
        indexName = SimpleIdentifier()
        <ON>
        (
            columnList = ParenthesizedSimpleIdentifierList()
        |
            column = SimpleIdentifier()
            {
                columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
            }
        )
        (
            <USING> indexType = SimpleIdentifier()
        |
            { indexType = null; }
        )
        {
            return new SqlAlterTableAddIndex(s.end(this), table, columnList, unique, indexType, indexName);
        }
    |
        <DROP> <INDEX>
        indexName = SimpleIdentifier()
        {
            return new SqlAlterTableDropIndex(s.end(this), table, indexName);
        }
    |
        <MODIFY> <COLUMN>
        column = SimpleIdentifier()
        statement = AlterTableModifyColumn(s, table, column)
        {
            return statement;
        }

    |
        <PARTITION> <BY>
        partitionType = SimpleIdentifier()
        <LPAREN> partitionColumn = SimpleIdentifier() <RPAREN>
        (
            [  <PARTITIONS> numPartitions = UnsignedIntLiteral() ]
        )
        {
            return new SqlAlterTableAddPartitions(s.end(this), table, partitionColumn, partitionType, numPartitions);
        }

    |
        <MERGE> <PARTITIONS>
        {
            return new SqlAlterTableMergePartitions(s.end(this), table);
        }
    )
}

/**
* Parses the MODIFY COLUMN part of an ALTER TABLE statement.
*/
SqlAlterTableModifyColumn AlterTableModifyColumn(Span s, SqlIdentifier table, SqlIdentifier column) :
{
    SqlDataTypeSpec type = null;
    Boolean nullable = null;
    SqlIdentifier beforeColumn = null;
    SqlIdentifier afterColumn = null;
    SqlNode defaultValue = null;
    Boolean dropDefault = null;
    String collation = null;
}
{
    (
        <SET> <NOT> <NULL>
        { nullable = false; }
    |
        <DROP> <NOT> <NULL>
        { nullable = true; }
    |
        <SET> <TYPE>
        type = DataType()
    |
        <SET> <POSITION>
        (
            <BEFORE>
            beforeColumn = SimpleIdentifier()
        |
            <AFTER>
            afterColumn = SimpleIdentifier()
        )
    |
        <SET> <COLLATION>
        (
            <CASE> <SENSITIVE> { collation = "CASE SENSITIVE"; }
        |
            <CASE> <INSENSITIVE> { collation = "CASE INSENSITIVE"; }
        )
    |
        <SET><DEFAULT_> defaultValue = Expression(ExprContext.ACCEPT_NONCURSOR)
    |
        <DROP> <DEFAULT_> { dropDefault = true; }
    )
    {
        return new SqlAlterTableModifyColumn(s.end(this), table, column, type, nullable, beforeColumn, afterColumn, collation, defaultValue, dropDefault);
    }
}


SqlAlterConfig SqlAlterConfig(Span s) :
{
    final SqlNode key;
    final SqlNode value;
}
{
    <CONFIG> key = Expression(ExprContext.ACCEPT_NONCURSOR)
    <SET> value = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterConfig(s.end(this), key, value);
    }
}


SqlAlterStoresAdd SqlAlterStoresAdd(Span s) :
{
    final SqlNode storeName;
    final SqlNode adapterName;
    final SqlNode config;
}
{
    <STORES> <ADD> storeName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <USING> adapterName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <WITH> config = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterStoresAdd(s.end(this), storeName, adapterName, config);
    }
}


SqlAlterStoresDrop SqlAlterStoresDrop(Span s) :
{
    final SqlNode storeName;
}
{
    <STORES> <DROP> storeName = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterStoresDrop(s.end(this), storeName);
    }
}