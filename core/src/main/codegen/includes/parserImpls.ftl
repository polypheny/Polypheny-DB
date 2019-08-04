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
        owner = CompoundIdentifier()
        {
            return new SqlAlterSchemaOwner(s.end(this), schema, owner);
        }
    )
}

