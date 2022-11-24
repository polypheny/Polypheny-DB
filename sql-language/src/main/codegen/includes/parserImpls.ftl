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
        <TRANSFER>
        name = CompoundIdentifier()
        {
            return new SqlAlterSchemaTransferTable(s.end(this), name, schema);
        }
    |
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
* Parses a ALTER VIEW statement.
**/
SqlAlterView SqlAlterView(Span s) :
{
    final SqlIdentifier view;
    final SqlIdentifier name;
    final SqlIdentifier column;
}
{
    <VIEW>
    view = CompoundIdentifier()
    (
        <RENAME><TO>
        name = SimpleIdentifier()
        {
            return new SqlAlterViewRename(s.end(this), view, name);
        }
    |
        <RENAME> <COLUMN>
        column = SimpleIdentifier()
        <TO>
        name = SimpleIdentifier()
        {
            return new SqlAlterViewRenameColumn(s.end(this), view, column, name);
        }
    )
}

/**
* Parses a ALTER MATERIALIZED VIEW statement.
**/
SqlAlterMaterializedView SqlAlterMaterializedView(Span s) :
{
    final SqlIdentifier materializedview;
    final SqlIdentifier name;
    final SqlIdentifier column;
    final SqlIdentifier store;
    final SqlIdentifier indexName;
    final SqlNodeList columnList;
    final SqlIdentifier indexMethod;
    final boolean unique;
    final SqlIdentifier storeName;

}
{
    <MATERIALIZED><VIEW>
    materializedview = CompoundIdentifier()
    (
        <RENAME><TO>
            name = SimpleIdentifier()
            {
            return new SqlAlterMaterializedViewRename(s.end(this), materializedview, name);
            }
    |
        <RENAME> <COLUMN>
            column = SimpleIdentifier()
            <TO>
            name = SimpleIdentifier()
            {
                return new SqlAlterMaterializedViewRenameColumn(s.end(this), materializedview, column, name);
            }
    |
        <FRESHNESS><MANUAL>
            {
                return new SqlAlterMaterializedViewFreshnessManual(s.end(this), materializedview);
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
            <USING> indexMethod = SimpleIdentifier()
            |
            { indexMethod = null; }
            )
            (
            <ON> <STORE> storeName = SimpleIdentifier()
            |
            { storeName = null; }
            )
            {
            return new SqlAlterMaterializedViewAddIndex(s.end(this), materializedview, columnList, unique, indexMethod, indexName, storeName);
            }
    |
        <DROP> <INDEX>
            indexName = SimpleIdentifier()
            {
            return new SqlAlterMaterializedViewDropIndex(s.end(this), materializedview, indexName);
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
    final SqlIdentifier indexMethod;
    final SqlIdentifier storeName;
    final String onUpdate;
    final String onDelete;
    final boolean unique;
    final SqlIdentifier physicalName;
    final SqlIdentifier partitionType;
    final SqlIdentifier partitionColumn;
    List<Integer> partitionList = new ArrayList<Integer>();
    int partitionIndex = 0;
    int numPartitionGroups = 0;
    int numPartitions = 0;
    List<SqlIdentifier> partitionNamesList = new ArrayList<SqlIdentifier>();
    SqlIdentifier partitionName = null;
    List< List<SqlNode>> partitionQualifierList = new ArrayList<List<SqlNode>>();
    List<SqlNode> partitionQualifiers = new ArrayList<SqlNode>();
    SqlNode partitionValues = null;
    SqlIdentifier tmpIdent = null;
    int tmpInt = 0;
    RawPartitionInformation rawPartitionInfo;
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
                <DEFAULT_>
                defaultValue = Literal()
            |
                defaultValue = ArrayConstructor()
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
            <AS>
            physicalName = SimpleIdentifier()
            (
                <DEFAULT_>
                defaultValue = Literal()
            |
                defaultValue = ArrayConstructor()
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
                return new SqlAlterSourceTableAddColumn(s.end(this), table, name, physicalName, defaultValue, beforeColumn, afterColumn);
            }
        )
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
                    <NONE> { onUpdate = "NONE"; }
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
                    <NONE> { onDelete = "NONE"; }
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
                (
                        partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                        (
                            <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                        )*
                    |
                        partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                        (
                            <COMMA> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                        )*
                )
                <RPAREN>
            ]
        {
            return new SqlAlterTableAddPlacement(s.end(this), table, columnList, store, partitionList, partitionNamesList);
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
                (
                        partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                        (
                            <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                        )*
                    |

                        partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                        (
                            <COMMA> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                        )*
               )
               <RPAREN>
            ]
            {
                return new SqlAlterTableModifyPlacement(s.end(this), table, columnList, store, partitionList, partitionNamesList);
            }
        )

    |
        <MODIFY>
        <PARTITIONS>
        <LPAREN>
            (
                    partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    (
                        <COMMA> partitionIndex = UnsignedIntLiteral() { partitionList.add(partitionIndex); }
                    )*
                |

                    partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                    (
                         <COMMA> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                    )*
            )
        <RPAREN>
        <ON>
        <STORE> store = SimpleIdentifier()
        {
            return new SqlAlterTableModifyPartitions(s.end(this), table, store, partitionList, partitionNamesList);
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
            <USING> indexMethod = SimpleIdentifier()
        |
            { indexMethod = null; }
        )
        (
            <ON> <STORE> storeName = SimpleIdentifier()
        |
            { storeName = null; }
        )
        {
            return new SqlAlterTableAddIndex(s.end(this), table, columnList, unique, indexMethod, indexName, storeName);
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
                    (
                            partitionType = SimpleIdentifier()
                        |
                            <RANGE> { partitionType = new SqlIdentifier( "RANGE", s.end(this) );}

                        |
                            <TEMPERATURE> { partitionType = new SqlIdentifier( "TEMPERATURE", s.end(this) );
                                    rawPartitionInfo = new RawTemperaturePartitionInformation();
                                    rawPartitionInfo.setPartitionType( partitionType );
                                    }
                                    <LPAREN> partitionColumn = SimpleIdentifier() { rawPartitionInfo.setPartitionColumn( partitionColumn ); } <RPAREN>
                                    <LPAREN>
                                        <PARTITION> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                                                <VALUES> <LPAREN>
                                                            partitionValues = Literal()
                                                            {
                                                                 partitionQualifiers.add(partitionValues);
                                                                 ((RawTemperaturePartitionInformation)rawPartitionInfo).setHotAccessPercentageIn( partitionValues );
                                                            } <PERCENT_REMAINDER>
                                                <RPAREN> {partitionQualifierList.add(partitionQualifiers); partitionQualifiers = new ArrayList<SqlNode>();}
                                        <COMMA>
                                        <PARTITION> partitionName = SimpleIdentifier() { partitionNamesList.add(partitionName); }
                                                <VALUES> <LPAREN>
                                                            partitionValues = Literal()
                                                            {
                                                                partitionQualifiers.add(partitionValues);
                                                                ((RawTemperaturePartitionInformation)rawPartitionInfo).setHotAccessPercentageOut( partitionValues );
                                                            } <PERCENT_REMAINDER>
                                                    <RPAREN> {partitionQualifierList.add(partitionQualifiers); partitionQualifiers = new ArrayList<SqlNode>();}
                                                    <RPAREN>
                                                        <USING> <FREQUENCY>
                                                                (
                                                                    <ALL> { ((RawTemperaturePartitionInformation)rawPartitionInfo).setAccessPattern( new SqlIdentifier( "ALL", s.end(this) ) ); tmpIdent = null; }
                                                                |
                                                                    <WRITE> { ((RawTemperaturePartitionInformation)rawPartitionInfo).setAccessPattern( new SqlIdentifier( "WRITE", s.end(this) ) ); tmpIdent = null; }
                                                                |
                                                                    <READ> { ((RawTemperaturePartitionInformation)rawPartitionInfo).setAccessPattern( new SqlIdentifier( "READ", s.end(this) ) ); tmpIdent = null;}
                                                                )
                                                            <INTERVAL>
                                                                    tmpInt = UnsignedIntLiteral() { ((RawTemperaturePartitionInformation)rawPartitionInfo).setInterval( tmpInt ); tmpInt = 0; }
                                                                    tmpIdent = SimpleIdentifier() { ((RawTemperaturePartitionInformation)rawPartitionInfo).setIntervalUnit( tmpIdent ); tmpIdent = null; }
                                                        <WITH>  numPartitions = UnsignedIntLiteral() {rawPartitionInfo.setNumPartitions( numPartitions );}
                                                                    tmpIdent = SimpleIdentifier() {
                                                                    ((RawTemperaturePartitionInformation)rawPartitionInfo).setInternalPartitionFunction( tmpIdent ); tmpIdent = null;
                                                            } <PARTITIONS>
                                                                    {
                                                                    rawPartitionInfo.setPartitionNamesList( CoreUtil.toNodeList( partitionNamesList, Identifier.class ) );
                                                                    rawPartitionInfo.setPartitionQualifierList( SqlUtil.toNodeListList( partitionQualifierList ) );

                                                                return new SqlAlterTableAddPartitions(s.end(this), table, partitionColumn, partitionType, numPartitionGroups, numPartitions, partitionNamesList, partitionQualifierList, rawPartitionInfo);
                                                                            }
                    )

        <LPAREN> partitionColumn = SimpleIdentifier() <RPAREN>
        [
                (
                        <PARTITIONS> numPartitionGroups = UnsignedIntLiteral()
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
                                                    (
                                                        <COMMA> partitionValues = Literal() { partitionQualifiers.add(partitionValues); }
                                                    )*
                                            <RPAREN> {partitionQualifierList.add(partitionQualifiers); partitionQualifiers = new ArrayList<SqlNode>();}
                                )*
                            <RPAREN>
                )
        ]
        {
            rawPartitionInfo = new RawPartitionInformation();
            return new SqlAlterTableAddPartitions(s.end(this), table, partitionColumn, partitionType, numPartitionGroups, numPartitions, partitionNamesList, partitionQualifierList, rawPartitionInfo);
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


SqlAlterAdaptersAdd SqlAlterAdaptersAdd(Span s) :
{
    final SqlNode uniqueName;
    final SqlNode adapterName;
    final SqlNode config;
}
{
    <ADAPTERS> <ADD> uniqueName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <USING> adapterName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <WITH> config = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterAdaptersAdd(s.end(this), uniqueName, adapterName, config);
    }
}


SqlAlterAdaptersDrop SqlAlterAdaptersDrop(Span s) :
{
    final SqlNode uniqueName;
}
{
    <ADAPTERS> <DROP> uniqueName = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterAdaptersDrop(s.end(this), uniqueName);
    }
}


SqlAlterInterfacesAdd SqlAlterInterfacesAdd(Span s) :
{
    final SqlNode uniqueName;
    final SqlNode clazzName;
    final SqlNode config;
}
{
    <INTERFACES> <ADD> uniqueName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <USING> clazzName = Expression(ExprContext.ACCEPT_NONCURSOR)
    <WITH> config = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterInterfacesAdd(s.end(this), uniqueName, clazzName, config);
    }
}


SqlAlterInterfacesDrop SqlAlterInterfacesDrop(Span s) :
{
    final SqlNode uniqueName;
}
{
    <INTERFACES> <DROP> uniqueName = Expression(ExprContext.ACCEPT_NONCURSOR)
    {
        return new SqlAlterInterfacesDrop(s.end(this), uniqueName);
    }
}