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
    final SqlIdentifier beforeColumn;
    final SqlIdentifier afterColumn;
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
                <BEFORE> { beforeColumn = SimpleIdentifier(); afterColumn = null; }
                |
                <AFTER> { afterColumn = SimpleIdentifier(); beforeColumn = null; }
                |
                { afterColumn = null; beforeColumn = null; }
            )
            {
                return new SqlAlterTableAddColumn(s.end(this), table, name, type, nullable, beforeColumn, afterColumn);
            }
        |
            <DROP> <COLUMN>
            column = SimpleIdentifier()
            {
                return new SqlAlterTableDropColumn(s.end(this), table, column);
            }
    )
}

