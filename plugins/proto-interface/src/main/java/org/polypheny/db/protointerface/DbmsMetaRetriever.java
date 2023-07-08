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
import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey.CatalogForeignKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey.CatalogPrimaryKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.protointerface.proto.Column;
import org.polypheny.db.protointerface.proto.ColumnsResponse;
import org.polypheny.db.protointerface.proto.Database;
import org.polypheny.db.protointerface.proto.DatabasesResponse;
import org.polypheny.db.protointerface.proto.ExportedKeysResponse;
import org.polypheny.db.protointerface.proto.ForeignKey;
import org.polypheny.db.protointerface.proto.ImportedKeysResponse;
import org.polypheny.db.protointerface.proto.Namespace;
import org.polypheny.db.protointerface.proto.NamespacesResponse;
import org.polypheny.db.protointerface.proto.PrimaryKey;
import org.polypheny.db.protointerface.proto.PrimaryKeysResponse;
import org.polypheny.db.protointerface.proto.Table;
import org.polypheny.db.protointerface.proto.TableType;
import org.polypheny.db.protointerface.proto.TableTypesResponse;
import org.polypheny.db.protointerface.proto.TablesResponse;
import org.polypheny.db.protointerface.proto.Type;
import org.polypheny.db.protointerface.proto.TypesResponse;
import org.polypheny.db.type.PolyType;

public class DbmsMetaRetriever {


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

    public static synchronized TablesResponse getTables( String namespacePattern, String tablePattern, List<String> tableTypes ) {
        final List<LogicalTable> tables = getLogicalTables( namespacePattern, tablePattern, tableTypes );
        TablesResponse.Builder responseBuilder = TablesResponse.newBuilder();
        tables.forEach( logicalTable -> responseBuilder.addTables( getTableMeta( logicalTable ) ) );
        return responseBuilder.build();
    }


    @NotNull
    private static List<LogicalTable> getLogicalTables( Pattern namespacePattern, Pattern tablePattern ) {
        return Catalog.getInstance().getSnapshot().rel().getTables( namespacePattern, tablePattern );
    }


    @NotNull
    private static List<LogicalTable> getLogicalTables( String namespacePattern, String tablePattern, List<String> tableTypes ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        List<EntityType> entityTypes = tableTypes.stream().map( EntityType::getByName ).collect( Collectors.toList() );
        return getLogicalTables( catalogNamespacePattern, catalogTablePattern ).stream()
                .filter( t -> entityTypes.contains( t.entityType ) ).collect( Collectors.toList() );
    }


    private static Pattern getPatternOrNull( String pattern ) {
        return pattern == null ? null : new Pattern( pattern );
    }


    private static Table getTableMeta( LogicalTable logicalTable ) {
        Serializable[] parameters = logicalTable.getParameterArray();
        return Table.newBuilder()
                .setSourceDatabaseName( parameters[0].toString() )
                .setNamespaceName( parameters[1].toString() )
                .setTableName( parameters[2].toString() )
                .setTableType( parameters[3].toString() )
                .setOwnerName( parameters[10].toString() )
                .build();
    }


    private static List<LogicalColumn> getLogicalColumns( String namespacePattern, String tablePattern, String columnPattern ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        Pattern catalogColumnPattern = getPatternOrNull( columnPattern );
        return getLogicalTables( catalogNamespacePattern, catalogTablePattern ).stream()
                .flatMap( t -> getLogicalColumns( catalogTablePattern, catalogColumnPattern ).stream() )
                .collect( Collectors.toList() );
    }


    private static List<LogicalColumn> getLogicalColumns( Pattern catalogTablePattern, Pattern catalogColumnPattern ) {
        return Catalog.getInstance().getSnapshot().rel().getColumns( catalogTablePattern, catalogColumnPattern );
    }


    public static synchronized ColumnsResponse getColumns( String namespacePattern, String tablePattern, String columnPattern ) {
        final List<LogicalColumn> columns = getLogicalColumns( namespacePattern, tablePattern, columnPattern );
        ColumnsResponse.Builder responseBuilder = ColumnsResponse.newBuilder();
        columns.forEach( logicalColumn -> responseBuilder.addColumns( getColumnMeta( logicalColumn ) ) );
        return responseBuilder.build();
    }


    private static Column getColumnMeta( LogicalColumn logicalColumn ) {
        Serializable[] parameters = logicalColumn.getParameterArray();
        Column.Builder columnBuilder = Column.newBuilder();
        columnBuilder.setDatabaseName( parameters[0].toString() );
        columnBuilder.setNamespaceName( parameters[1].toString() );
        columnBuilder.setTableName( parameters[2].toString() );
        columnBuilder.setColumnName( parameters[3].toString() );
        columnBuilder.setTypeName( parameters[5].toString() );
        columnBuilder.setTypeLength( Integer.parseInt( parameters[6].toString() ) );
        columnBuilder.setTypeScale( Integer.parseInt( parameters[8].toString() ) );
        columnBuilder.setIsNullable( parameters[10].toString().equals( "1" ) );
        Optional.of( parameters[12] ).ifPresent( p -> columnBuilder.setDefaultValueAsString( p.toString() ) );
        columnBuilder.setColumnIndex( Integer.parseInt( parameters[16].toString() ) );
        Optional.of( parameters[18] ).ifPresent( p -> columnBuilder.setCollation( p.toString() ) );
        return columnBuilder.build();
    }


    public static synchronized NamespacesResponse getNamespaces( String namespacePattern ) {
        List<LogicalNamespace> namespaces = getLogicalNamespaces( namespacePattern );
        NamespacesResponse.Builder responseBuilder = NamespacesResponse.newBuilder();
        namespaces.forEach( namespace -> responseBuilder.addNamespaces( getNamespaceMeta( namespace ) ) );
        return responseBuilder.build();
    }


    private static List<LogicalNamespace> getLogicalNamespaces( String namespacePattern ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        return Catalog.getInstance().getSnapshot().getNamespaces( catalogNamespacePattern );
    }


    private static Namespace getNamespaceMeta( LogicalNamespace logicalNamespace ) {
        Serializable[] parameters = logicalNamespace.getParameterArray();
        Namespace.Builder namespaceBuilder = Namespace.newBuilder();
        namespaceBuilder.setNamespaceName( parameters[0].toString() );
        namespaceBuilder.setDatabaseName( parameters[1].toString() );
        namespaceBuilder.setOwnerName( parameters[2].toString() );
        Optional.ofNullable( parameters[3] ).ifPresent( p -> namespaceBuilder.setNamespaceType( p.toString() ) );
        return namespaceBuilder.build();
    }


    public static synchronized DatabasesResponse getDatabases() {
        Database database = Database.newBuilder()
                .setDatabaseName( Catalog.DATABASE_NAME )
                .setOwnerName( "system" )
                .setDefaultNamespaceName( Catalog.defaultNamespaceName )
                .build();
        return DatabasesResponse.newBuilder()
                .addDatabases( database )
                .build();
    }


    public static synchronized TableTypesResponse getTableTypes() {
        List<String> tableTypes = Arrays.stream( EntityType.values() ).map( EntityType::name ).collect( Collectors.toList() );
        TableTypesResponse.Builder responseBuilder = TableTypesResponse.newBuilder();
        tableTypes.forEach( tableType -> responseBuilder.addTableTypes( getTableTypeMeta( tableType ) ) );
        return responseBuilder.build();
    }


    private static TableType getTableTypeMeta( String tableType ) {
        return TableType.newBuilder().setTableType( tableType ).build();
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
*/


    public static synchronized PrimaryKeysResponse getPrimaryKeys( String namespacePattern, String tablePattern ) {
        List<CatalogPrimaryKeyColumn> primaryKeyColumns = getPrimaryKeyColumns( namespacePattern, tablePattern );
        PrimaryKeysResponse.Builder responseBuilder = PrimaryKeysResponse.newBuilder();
        primaryKeyColumns.forEach( primaryKeyColumn -> responseBuilder.addPrimaryKeys( getPrimaryKeyColumnMeta( primaryKeyColumn ) ) );
        return responseBuilder.build();
    }


    private static List<CatalogPrimaryKeyColumn> getPrimaryKeyColumns( String namespacePattern, String tablePattern ) {
        Pattern cataloNamespacePattern = getPatternOrNull( namespacePattern );
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        return getLogicalTables( cataloNamespacePattern, catalogTablePattern ).stream()
                .filter( e -> e.primaryKey != null )
                .map( DbmsMetaRetriever::getLogicalPrimaryKey )
                .map( LogicalPrimaryKey::getCatalogPrimaryKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    private static LogicalPrimaryKey getLogicalPrimaryKey( LogicalTable logicalTable ) {
        return Catalog.getInstance().getSnapshot().rel().getPrimaryKey( logicalTable.primaryKey ).orElseThrow();
    }


    private static PrimaryKey getPrimaryKeyColumnMeta( CatalogPrimaryKeyColumn catalogPrimaryKeyColumn ) {
        Serializable[] parameters = catalogPrimaryKeyColumn.getParameterArray();
        PrimaryKey.Builder primaryKeyBuilder = PrimaryKey.newBuilder();
        Optional.ofNullable( parameters[0] ).ifPresent( p -> primaryKeyBuilder.setDatabaseName( p.toString() ) );
        Optional.ofNullable( parameters[1] ).ifPresent( p -> primaryKeyBuilder.setNamespaceName( p.toString() ) );
        primaryKeyBuilder.setTableName( parameters[2].toString() );
        primaryKeyBuilder.setColumnName( parameters[3].toString() );
        primaryKeyBuilder.setSequenceIndex( Integer.parseInt( parameters[4].toString() ) );
        return primaryKeyBuilder.build();
    }


    public static synchronized ImportedKeysResponse getImportedKeys( String schemaPattern, String tablePattern ) {
        List<CatalogForeignKeyColumn> foreignKeyColumns = getForeignKeyColumns( schemaPattern, tablePattern );
        ImportedKeysResponse.Builder responseBuilder = ImportedKeysResponse.newBuilder();
        foreignKeyColumns.forEach( foreignKeyColumn -> responseBuilder.addImportedKeys( getForeignKeyColumnMeta( foreignKeyColumn ) ) );
        return responseBuilder.build();
    }


    private static List<CatalogForeignKeyColumn> getForeignKeyColumns( String schemaPattern, String tablePattern ) {
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        Pattern catalogSchemaPattern = getPatternOrNull( schemaPattern );
        return getLogicalTables( catalogSchemaPattern, catalogTablePattern ).stream()
                .map( CatalogEntity::getId )
                .map( DbmsMetaRetriever::getLogicalForeignKeysOf )
                .flatMap( Collection::stream )
                .map( LogicalForeignKey::getCatalogForeignKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    private static List<LogicalForeignKey> getLogicalForeignKeysOf( long entityId ) {
        return Catalog.getInstance().getSnapshot().rel().getForeignKeys( entityId );
    }


    private static ForeignKey getForeignKeyColumnMeta( CatalogForeignKeyColumn catalogForeignKeyColumn ) {
        Serializable[] parameters = catalogForeignKeyColumn.getParameterArray();
        ForeignKey.Builder importedKeyBuilder = ForeignKey.newBuilder();
        Optional.ofNullable( parameters[0] ).ifPresent( p -> importedKeyBuilder.setReferencedDatabaseName( p.toString() ) );
        Optional.ofNullable( parameters[1] ).ifPresent( p -> importedKeyBuilder.setReferencedNamespaceName( p.toString() ) );
        importedKeyBuilder.setReferencedTableName( parameters[2].toString() );
        importedKeyBuilder.setReferencedColumnName( parameters[3].toString() );
        Optional.ofNullable( parameters[4] ).ifPresent( p -> importedKeyBuilder.setForeignDatabaseName( p.toString() ) );
        Optional.ofNullable( parameters[5] ).ifPresent( p -> importedKeyBuilder.setForeignNamespaceName( p.toString() ) );
        importedKeyBuilder.setForeignTableName( parameters[6].toString() );
        importedKeyBuilder.setForeignColumnName( parameters[7].toString() );
        importedKeyBuilder.setSequenceIndex( Integer.parseInt( parameters[8].toString() ) );
        importedKeyBuilder.setUpdateRule( Integer.parseInt( parameters[9].toString() ) );
        importedKeyBuilder.setDeleteRule( Integer.parseInt( parameters[10].toString() ) );
        importedKeyBuilder.setKeyName( parameters[11].toString() );
        return importedKeyBuilder.build();
    }


    public static synchronized ExportedKeysResponse getExportedKeys( String schemaPattern, String tablePattern ) {
        List<CatalogForeignKeyColumn> exportedKeyColumns = getExportedKeyColumns( schemaPattern, tablePattern );
        ExportedKeysResponse.Builder responseBuilder = ExportedKeysResponse.newBuilder();
        exportedKeyColumns.forEach( foreignKeyColumn -> responseBuilder.addExportedKeys( getForeignKeyColumnMeta( foreignKeyColumn ) ) );
        return responseBuilder.build();
    }


    @SuppressWarnings("Duplicates")
    private static List<CatalogForeignKeyColumn> getExportedKeyColumns( String schemaPattern, String tablePattern ) {
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        Pattern catalogSchemaPattern = getPatternOrNull( schemaPattern );
        return getLogicalTables( catalogSchemaPattern, catalogTablePattern ).stream()
                .map( CatalogEntity::getId )
                .map( DbmsMetaRetriever::getExportedKeysOf )
                .flatMap( Collection::stream )
                .map( LogicalForeignKey::getCatalogForeignKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    private static List<LogicalForeignKey> getExportedKeysOf( long entityId ) {
        return Catalog.getInstance().getSnapshot().rel().getExportedKeys( entityId );
    }


    /*

        @SuppressWarnings("Duplicates")
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

    */
    public static synchronized TypesResponse getTypes() {
        TypesResponse.Builder responseBuilder = TypesResponse.newBuilder();
        Arrays.stream( PolyType.values() ).forEach( polyType -> responseBuilder.addTypes( getTypeMeta( polyType ) ) );
        return responseBuilder.build();
    }


    private static Type getTypeMeta( PolyType polyType ) {
        AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
        Type.Builder typeBuilder = Type.newBuilder();
        typeBuilder.setTypeName( polyType.getName() );
        typeBuilder.setPrecision( typeSystem.getMaxPrecision( polyType ) );
        typeBuilder.setLiteralPrefix( typeSystem.getLiteral( polyType, true ) );
        typeBuilder.setLiteralSuffix( typeSystem.getLiteral( polyType, false ) );
        typeBuilder.setIsCaseSensitive( typeSystem.isCaseSensitive( polyType ) );
        typeBuilder.setIsSearchable( DatabaseMetaData.typeSearchable );
        typeBuilder.setIsAutoIncrement( typeSystem.isAutoincrement( polyType ) );
        typeBuilder.setMinScale( polyType.getMinScale() );
        typeBuilder.setMaxScale( typeSystem.getMaxScale( polyType ) );
        typeBuilder.setRadix( typeSystem.getNumTypeRadix( polyType ) );
        return typeBuilder.build();
    }



/*
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