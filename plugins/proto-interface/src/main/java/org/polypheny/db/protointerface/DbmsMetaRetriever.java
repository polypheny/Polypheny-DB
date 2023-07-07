/*
 * Copyright 2019-2023 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.protointerface;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.protointerface.proto.Table;
import org.polypheny.db.protointerface.proto.TableTypesResponse;
import org.polypheny.db.protointerface.proto.TablesResponse;

public class DbmsMetaRetriever {

    private static final int TABLE_CAT_INDEX = 0;
    private static final int TABLE_SCHEM_INDEX = 1;
    private static final int TABLE_NAME_INDEX = 2;
    private static final int TABLE_TYPE_INDEX = 3;
    private static final int TABLE_OWNER_INDEX = 10;


    /**
     * public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
     * final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
     * synchronized ( connection ) {
     * if ( log.isTraceEnabled() ) {
     * log.trace( "getDatabaseProperties( ConnectionHandle {} )", ch );
     * }
     *
     * final Map<DatabaseProperty, Object> map = new HashMap<>();
     * // TODO
     *
     * log.error( "[NOT IMPLEMENTED YET] getDatabaseProperties( ConnectionHandle {} )", ch );
     * return map;
     * }
     * }
     */
    // TODO TH: typeList is ignored
    public static synchronized TablesResponse getTables( String schemaPattern, String tablePattern, List<String> tableTypes ) {
        final List<LogicalTable> tables = getLogicalTables( schemaPattern, tablePattern, tableTypes );
        TablesResponse.Builder responseBuilder = TablesResponse.newBuilder();
        tables.forEach( logicalTable -> responseBuilder.addTables( getTableMeta( logicalTable ) ) );
        return responseBuilder.build();
    }


    @NotNull
    private static List<LogicalTable> getLogicalTables( String schemaPattern, String tablePattern, List<String> tableTypes ) {
        Pattern catalogSchemaPattern = schemaPattern == null ? null : new Pattern( schemaPattern );
        Pattern catalogTablePattern = tablePattern == null ? null : new Pattern( tablePattern );
        List<EntityType> entityTypes = tableTypes.stream().map( EntityType::getByName ).collect( Collectors.toList() );
        return Catalog.getInstance().getSnapshot().rel().getTables( catalogSchemaPattern, catalogTablePattern )
                .stream().filter( t -> entityTypes.contains( t.entityType ) ).collect( Collectors.toList() );
    }


    private static Table getTableMeta( LogicalTable logicalTable ) {
        Serializable[] parameters = logicalTable.getParameterArray();
        return Table.newBuilder()
                .setSourceDatabaseName( parameters[TABLE_CAT_INDEX].toString() )
                .setNamespaceName( parameters[TABLE_SCHEM_INDEX].toString() )
                .setTableName( parameters[TABLE_NAME_INDEX].toString() )
                .setTableType( parameters[TABLE_TYPE_INDEX].toString() )
                .setOwnerName( parameters[TABLE_OWNER_INDEX].toString() )
                .build();
    }
/*
    public MetaResultSet getColumns( final ConnectionHandle ch, final String database, final Pat schemaPattern, final Pat tablePattern, final Pat columnPattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getAllocColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, database, schemaPattern, tablePattern, columnPattern );
            }
            final List<LogicalColumn> columns = getLogicalTables( schemaPattern, tablePattern ).stream().flatMap( t -> catalog.getSnapshot().rel().getColumns(
                    (tablePattern == null || tablePattern.s == null) ? null : new Pattern( tablePattern.s ),
                    (columnPattern == null || columnPattern.s == null) ? null : new Pattern( columnPattern.s )
            ).stream() ).collect( Collectors.toList() );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( columns ),
                    PrimitiveCatalogColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",  // the database name
                    "TABLE_SCHEM",        // the schema name
                    "TABLE_NAME",         // the table name
                    "COLUMN_NAME",        // the column name
                    "DATA_TYPE",          // The SQL data type from java.sql.Types.
                    "TYPE_NAME",          // The name of the data type.
                    "COLUMN_SIZE",        // The length of the column (number of chars in a string or number of digits in a numerical data type).
                    "BUFFER_LENGTH",      // Transfer size of the data. --> not used, always null
                    "DECIMAL_DIGITS",     // The number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
                    "NUM_PREC_RADIX",     // The radix of the column. (typically either 10 or 2)
                    "NULLABLE",           // Indicates if the column is nullable. 1 means nullable
                    "REMARKS",            // The comments associated with the column. --> Polypheny-DB always returns null for this column
                    "COLUMN_DEF",         // The default value of the column.
                    "SQL_DATA_TYPE",      // This column is the same as the DATA_TYPE column, except for the datetime and SQL-92 interval data types. --> unused, always null
                    "SQL_DATETIME_SUB",   // Subtype code for datetime and SQL-92 interval data types. For other data types, this column returns NULL. --> unused, always null
                    "CHAR_OCTET_LENGTH",  // The maximum number of bytes in the column (only for char types) --> always null
                    "ORDINAL_POSITION",   // The index of the column within the table.
                    "IS_NULLABLE",        // Indicates if the column allows null values.
                    // Polypheny-DB specific extensions:
                    "COLLATION"
            );
        }
    }


    public MetaResultSet getSchemas( final ConnectionHandle ch, final String database, final Pat schemaPattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getNamespaces( ConnectionHandle {}, String {}, Pat {} )", ch, database, schemaPattern );
            }
            final List<LogicalNamespace> schemas = catalog.getSnapshot().getNamespaces(
                    (schemaPattern == null || schemaPattern.s == null) ? null : new Pattern( schemaPattern.s )
            );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( schemas ),
                    PrimitiveCatalogSchema.class,
                    // According to JDBC standard:
                    "TABLE_SCHEM",
                    "TABLE_CATALOG",
                    // Polypheny-DB specific extensions:
                    "OWNER",
                    "SCHEMA_TYPE"
            );
        }
    }


    public MetaResultSet getCatalogs( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getCatalogs( ConnectionHandle {} )", ch );
            }
            //final List<CatalogDatabase> databases = Linq4j.asEnumerable( new String[]{ "APP", "system", "public" } );
            List<Object> databases = Collections.singletonList( new Serializable[]{ Catalog.DATABASE_NAME, "system", Catalog.defaultNamespaceName } );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    Linq4j.asEnumerable( databases ),
                    PrimitiveCatalogDatabase.class,
                    // According to JDBC standard:
                    "TABLE_CAT",
                    // Polypheny-DB specific extensions:
                    "OWNER",
                    "DEFAULT_SCHEMA"
            );
        }
    }
*/


    public static synchronized TableTypesResponse getTableTypes() {
        List<String> tableTypes = Arrays.stream( EntityType.values() ).map( EntityType::name ).collect( Collectors.toList() );
        return TableTypesResponse.newBuilder().addAllTableTypes( tableTypes ).build();
    }


/*
    public MetaResultSet getProcedures( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
            return null;
        }
    }


    public MetaResultSet getProcedureColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
            return null;
        }
    }


    public MetaResultSet getColumnPrivileges( final ConnectionHandle ch, final String catalog, final String schema, final String table, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
            return null;
        }
    }


    public MetaResultSet getTablePrivileges( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            return null;
        }
    }


    public MetaResultSet getBestRowIdentifier( final ConnectionHandle ch, final String catalog, final String schema, final String table, final int scope, final boolean nullable ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
            }

            log.error( "[NOT IMPLEMENTED YET] getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
            return null;
        }
    }


    public MetaResultSet getVersionColumns( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
            }

            log.error( "[NOT IMPLEMENTED YET] getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
            return null;
        }
    }


    public MetaResultSet getPrimaryKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<CatalogPrimaryKeyColumn> primaryKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                if ( catalogTable.primaryKey != null ) {
                    final LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
                    primaryKeyColumns.addAll( primaryKey.getCatalogPrimaryKeyColumns() );
                }
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( primaryKeyColumns ),
                    PrimitiveCatalogPrimaryKeyColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",  // database name
                    "TABLE_SCHEM",        // schema name
                    "TABLE_NAME",         // table name
                    "COLUMN_NAME",        // column name
                    "KEY_SEQ",            // Sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
                    "PK_NAME"             // the name of the primary key --> always null (primary keys have no name in Polypheny-DB)
            );
        }
    }


    @SuppressWarnings("Duplicates")

    public MetaResultSet getImportedKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<CatalogForeignKeyColumn> foreignKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                List<LogicalForeignKey> importedKeys = catalog.getSnapshot().rel().getForeignKeys( catalogTable.id );
                importedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( foreignKeyColumns ),
                    PrimitiveCatalogForeignKeyColumn.class,
                    // According to JDBC standard:
                    "PKTABLE_CAT",    // The name of the database that contains the table with the referenced primary key.
                    "PKTABLE_SCHEM",          // The name of the schema that contains the table with the referenced primary key.
                    "PKTABLE_NAME",           // The name of the table with the referenced primary key.
                    "PKCOLUMN_NAME",          // The column name of the primary key being imported.
                    "FKTABLE_CAT",            // The name of the database that contains the table with the foreign key.
                    "FKTABLE_SCHEM",          // The name of the schema that contains the table with the foreign key.
                    "FKTABLE_NAME",          // The name of the table containing the foreign key.
                    "FKCOLUMN_NAME",          // The column name of the foreign key.
                    "KEY_SEQ",                // The sequence number of the column in a multi-column primary key.
                    "UPDATE_RULE",            // What happens to a foreign key when the primary key is updated.
                    "DELETE_RULE",            // What happens to a foreign key when the primary key is deleted.
                    "FK_NAME",                // The name of the foreign key.
                    "PK_NAME",                // The name of the primary key.
                    "DEFERRABILITY"           // Indicates if the evaluation of the foreign key constraint can be deferred until a commit. --> always null
            );
        }
    }


    @SuppressWarnings("Duplicates")

    public MetaResultSet getExportedKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );

            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<CatalogForeignKeyColumn> foreignKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                List<LogicalForeignKey> exportedKeys = catalog.getSnapshot().rel().getExportedKeys( catalogTable.id );
                exportedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( foreignKeyColumns ),
                    PrimitiveCatalogForeignKeyColumn.class,
                    // According to JDBC standard:
                    "PKTABLE_CAT",            // The name of the database that contains the table with the referenced primary key.
                    "PKTABLE_SCHEM",          // The name of the schema that contains the table with the referenced primary key.
                    "PKTABLE_NAME",           // The name of the table with the referenced primary key.
                    "PKCOLUMN_NAME",          // The column name of the primary key being imported.
                    "FKTABLE_CAT",            // The name of the database that contains the table with the foreign key.
                    "FKTABLE_SCHEM",          // The name of the schema that contains the table with the foreign key.
                    "FKTABLE_NAME",           // The name of the table containing the foreign key.
                    "FKCOLUMN_NAME",          // The column name of the foreign key.
                    "KEY_SEQ",                // The sequence number of the column in a multi-column primary key.
                    "UPDATE_RULE",            // What happens to a foreign key when the primary key is updated.
                    "DELETE_RULE",            // What happens to a foreign key when the primary key is deleted.
                    "FK_NAME",                // The name of the foreign key.
                    "PK_NAME",                // The name of the primary key. --> always null
                    "DEFERRABILITY"           // Indicates if the evaluation of the foreign key constraint can be deferred until a commit.
            );
        }
    }


    public MetaResultSet getCrossReference( final ConnectionHandle ch, final String parentCatalog, final String parentSchema, final String parentTable, final String foreignCatalog, final String foreignSchema, final String foreignTable ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
            return null;
        }
    }


    public MetaResultSet getTypeInfo( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTypeInfo( ConnectionHandle {} )", ch );
            }
            final StatementHandle statementHandle = createStatement( ch );
            final AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
            final List<Object> objects = new LinkedList<>();
            for ( PolyType polyType : PolyType.values() ) {
                objects.add(
                        new Serializable[]{
                                polyType.getName(),
                                polyType.getJdbcOrdinal(),
                                typeSystem.getMaxPrecision( polyType ),
                                typeSystem.getLiteral( polyType, true ),
                                typeSystem.getLiteral( polyType, false ),
                                null,
                                (short) DatabaseMetaData.typeNullable, // All types are nullable
                                typeSystem.isCaseSensitive( polyType ),
                                (short) DatabaseMetaData.typeSearchable, // Making all type searchable; we may want to be specific and declare under PolyType
                                false,
                                false,
                                typeSystem.isAutoincrement( polyType ),
                                polyType.getName(),
                                (short) polyType.getMinScale(),
                                (short) typeSystem.getMaxScale( polyType ),
                                null,
                                null,
                                typeSystem.getNumTypeRadix( polyType ) == 0 ? null : typeSystem.getNumTypeRadix( polyType ) } );
            }
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    Linq4j.asEnumerable( objects ),
                    MetaTypeInfo.class,
                    "TYPE_NAME",   // The name of the data type.
                    "DATA_TYPE",           // The SQL data type from java.sql.Types.
                    "PRECISION",           // The maximum number of significant digits.
                    "LITERAL_PREFIX",      // Prefix used to quote a literal
                    "LITERAL_SUFFIX",      // Suffix used to quote a literal
                    "CREATE_PARAMS",       // Parameters used in creating the type --> not used, always null
                    "NULLABLE",            // Indicates if the column can contain a null value (1: means type can contain null, 0 not). --> Currently 1 for all types
                    "CASE_SENSITIVE",      // Indicates if the data type is case-sensitive. "true" if the type is case-sensitive; otherwise, "false".
                    "SEARCHABLE",          // Indicates if (and how) the column can be used in a SQL WHERE clause. 0: none, 1: char, 2: basic, 3: searchable
                    "UNSIGNED_ATTRIBUTE",  // Indicates the sign of the data type. "true" if the type is unsigned; otherwise, "false". --> Currently false for all types
                    "FIXED_PREC_SCALE",    // Indicates that the data type can be a money value. "true" if the data type is money type; otherwise, "false".
                    "AUTO_INCREMENT",      // Indicates that the data type can be automatically incremented. "true" if the type can be auto incremented; otherwise, "false".
                    "LOCAL_TYPE_NAME",     // The localized name of the data type. --> Same as TYPE_NAME
                    "MINIMUM_SCALE",       // The maximum number of digits to the right of the decimal point.
                    "MAXIMUM_SCALE",       // The minimum number of digits to the right of the decimal point.
                    "SQL_DATA_TYPE",       // Not used, always null
                    "SQL_DATETIME_SUB",    // Not used, always null
                    "NUM_PREC_RADIX" );    // The radix
        }
    }


    public MetaResultSet getIndexInfo( final ConnectionHandle ch, final String database, final String schema, final String table, final boolean unique, final boolean approximate ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, database, schema, table, unique, approximate );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<CatalogIndexColumn> catalogIndexColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                List<LogicalIndex> logicalIndexInfos = catalog.getSnapshot().rel().getIndexes( catalogTable.id, unique );
                logicalIndexInfos.forEach( info -> catalogIndexColumns.addAll( info.getCatalogIndexColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( catalogIndexColumns ),
                    PrimitiveCatalogIndexColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",    // The name of the database in which the specified table resides.
                    "TABLE_SCHEM",          // The name of the schema in which the specified table resides.
                    "TABLE_NAME",           // The name of the table in which the index resides.
                    "NON_UNIQUE",           // Indicates whether the index values can be non-unique.
                    "INDEX_QUALIFIER",      // --> currently always returns null
                    "INDEX_NAME",           // The name of the index.
                    "TYPE",                 // The type of the index. (integer between 0 and 3) --> currently always returns 0
                    "ORDINAL_POSITION",     // The ordinal position of the column in the index. The first column in the index is 1.
                    "COLUMN_NAME",          // The name of the column.
                    "ASC_OR_DESC",          // The order used in the collation of the index.--> currently always returns null
                    "CARDINALITY",          // The number of rows in the table or unique values in the index. --> currently always returns -1
                    "PAGES",                // The number of pages used to store the index or table. --> currently always returns null
                    "FILTER_CONDITION",     // The filter condition. --> currently always returns null
                    // Polypheny-DB specific extensions
                    "LOCATION",             // On which store the index is located. NULL indicates a Polystore Index.
                    "INDEX_TYPE"            // Polypheny-DB specific index type
            );
        }
    }


    public MetaResultSet getUDTs( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final int[] types ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
            }

            log.error( "[NOT IMPLEMENTED YET] getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
            return null;
        }
    }


    public MetaResultSet getSuperTypes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
            return null;
        }
    }


    public MetaResultSet getSuperTables( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            return null;
        }
    }


    public MetaResultSet getAttributes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final Pat attributeNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
            return null;
        }
    }


    public MetaResultSet getClientInfoProperties( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getClientInfoProperties( ConnectionHandle {} )", ch );
            }

            log.error( "[NOT IMPLEMENTED YET] getClientInfoProperties( ConnectionHandle {} )", ch );
            return null;
        }
    }


    public MetaResultSet getFunctions( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
            return null;
        }
    }


    public MetaResultSet getFunctionColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
            return null;
        }
    }


    public MetaResultSet getPseudoColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
            return null;
        }
    }
    */
}