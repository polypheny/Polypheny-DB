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
    final SqlIdentifier indexName;
    final SqlIdentifier indexType;
    final String onUpdate;
    final String onDelete;
    final boolean unique;
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
                column = SimpleIdentifier() {
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
                        column = SimpleIdentifier() {
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
                        column = SimpleIdentifier() {
                            columnList = new SqlNodeList(Arrays.asList( new SqlNode[]{ column }), s.end(this));
                        }
                    )
                    <REFERENCES>
                    refTable = CompoundIdentifier()
                    referencesList = ParenthesizedSimpleIdentifierList()
                    (
                        <ON> <UPDATE> (
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
                        <ON> <DELETE> (
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
            <DROP> <FOREIGN> <KEY>
            constraintName = SimpleIdentifier()
            {
                return new SqlAlterTableDropForeignKey(s.end(this), table, constraintName);
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
                column = SimpleIdentifier() {
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
            <SET> <DEFAULT_>
            defaultValue = Expression(ExprContext.ACCEPT_NONCURSOR)
        |
            <DROP> <DEFAULT_>
            { dropDefault = true; }
    )
    {
        return new SqlAlterTableModifyColumn(s.end(this), table, column, type, nullable, beforeColumn, afterColumn, collation, defaultValue, dropDefault);
    }
}
