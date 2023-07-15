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
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey.CatalogForeignKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalIndex.CatalogIndexColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey.CatalogPrimaryKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.protointerface.proto.*;
import org.polypheny.db.sql.language.SqlJdbcFunctionCall;
import org.polypheny.db.type.PolyType;

public class DbMetaRetriever {


    private static Pattern getPatternOrNull( String pattern ) {
        return pattern == null ? null : new Pattern( pattern );
    }


    private static List<LogicalNamespace> getLogicalNamespaces( String namespacePattern ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        return Catalog.getInstance().getSnapshot().getNamespaces( catalogNamespacePattern );
    }


    @NotNull
    private static List<LogicalTable> getLogicalTables( String namespacePattern, String tablePattern ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        return Catalog.getInstance().getSnapshot().rel().getTables( catalogNamespacePattern, catalogTablePattern );
    }


    @NotNull
    private static List<LogicalTable> getLogicalTables( String namespacePattern, String tablePattern, List<String> tableTypes ) {
        List<LogicalTable> logicalTables = getLogicalTables( namespacePattern, tablePattern );
        if ( tableTypes == null ) {
            return logicalTables;
        }
        Set<EntityType> entityTypes = getTableTypes( tableTypes );
        return logicalTables.stream().filter( t -> entityTypes.contains( t.entityType ) ).collect( Collectors.toList() );
    }


    private static List<LogicalColumn> getLogicalColumns( String tablePattern, String columnPattern ) {
        Pattern catalogTablePattern = getPatternOrNull( tablePattern );
        Pattern catalogColumnPattern = getPatternOrNull( columnPattern );
        return Catalog.getInstance().getSnapshot().rel().getColumns( catalogTablePattern, catalogColumnPattern );
    }


    private static List<LogicalColumn> getLogicalColumns( String namespacePattern, String tablePattern, String columnPattern ) {
        return getLogicalTables( namespacePattern, tablePattern ).stream()
                .flatMap( t -> getLogicalColumns( tablePattern, columnPattern ).stream() )
                .collect( Collectors.toList() );
    }


    private static LogicalPrimaryKey getLogicalPrimaryKey( LogicalTable logicalTable ) {
        return Catalog.getInstance().getSnapshot().rel().getPrimaryKey( logicalTable.primaryKey ).orElseThrow();
    }


    private static List<LogicalForeignKey> getLogicalForeignKeysOf( long entityId ) {
        return Catalog.getInstance().getSnapshot().rel().getForeignKeys( entityId );
    }


    private static List<LogicalForeignKey> getExportedKeysOf( long entityId ) {
        return Catalog.getInstance().getSnapshot().rel().getExportedKeys( entityId );
    }


    private static List<LogicalIndex> getLogicalIndexesOf( long entityId, boolean unique ) {
        return Catalog.getInstance().getSnapshot().rel().getIndexes( entityId, unique );
    }


    private static List<CatalogPrimaryKeyColumn> getPrimaryKeyColumns( String namespacePattern, String tablePattern ) {
        return getLogicalTables( namespacePattern, tablePattern ).stream()
                .filter( e -> e.primaryKey != null )
                .map( DbMetaRetriever::getLogicalPrimaryKey )
                .map( LogicalPrimaryKey::getCatalogPrimaryKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    private static List<CatalogForeignKeyColumn> getForeignKeyColumns( String namespacePattern, String tablePattern ) {
        return getLogicalTables( namespacePattern, tablePattern ).stream()
                .map( CatalogEntity::getId )
                .map( DbMetaRetriever::getLogicalForeignKeysOf )
                .flatMap( Collection::stream )
                .map( LogicalForeignKey::getCatalogForeignKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    @SuppressWarnings("Duplicates")
    private static List<CatalogForeignKeyColumn> getExportedKeyColumns( String namespacePattern, String tablePattern ) {
        return getLogicalTables( namespacePattern, tablePattern ).stream()
                .map( CatalogEntity::getId )
                .map( DbMetaRetriever::getExportedKeysOf )
                .flatMap( Collection::stream )
                .map( LogicalForeignKey::getCatalogForeignKeyColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    private static List<CatalogIndexColumn> getCatalogIndexColumns( String namespacePattern, String tablePattern, boolean unique ) {
        return getLogicalTables( namespacePattern, tablePattern ).stream()
                .map( CatalogEntity::getId )
                .map( i -> getLogicalIndexesOf( i, unique ) )
                .flatMap( Collection::stream )
                .map( LogicalIndex::getCatalogIndexColumns )
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    public static synchronized TablesResponse getTables( String namespacePattern, String tablePattern, List<String> tableTypes ) {
        final List<LogicalTable> tables = getLogicalTables( namespacePattern, tablePattern, tableTypes );
        TablesResponse.Builder responseBuilder = TablesResponse.newBuilder();
        tables.forEach( logicalTable -> responseBuilder.addTables( getTableMeta( logicalTable ) ) );
        return responseBuilder.build();
    }


    @NotNull
    private static Set<EntityType> getTableTypes( List<String> tableTypes ) {
        return tableTypes.stream().map( EntityType::getByName ).collect( Collectors.toSet() );
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
        Optional.ofNullable( parameters[6] ).ifPresent( p -> columnBuilder.setTypeLength( Integer.parseInt( p.toString() ) ) );
        Optional.ofNullable( parameters[8] ).ifPresent( p -> columnBuilder.setTypeScale( Integer.parseInt( p.toString() ) ) );
        columnBuilder.setIsNullable( parameters[10].toString().equals( "1" ) );
        Optional.ofNullable( parameters[12] ).ifPresent( p -> columnBuilder.setDefaultValueAsString( p.toString() ) );
        columnBuilder.setColumnIndex( Integer.parseInt( parameters[16].toString() ) );
        Optional.ofNullable( parameters[18] ).ifPresent( p -> columnBuilder.setCollation( p.toString() ) );
        return columnBuilder.build();
    }


    public static synchronized NamespacesResponse getNamespaces( String namespacePattern ) {
        List<LogicalNamespace> namespaces = getLogicalNamespaces( namespacePattern );
        NamespacesResponse.Builder responseBuilder = NamespacesResponse.newBuilder();
        namespaces.forEach( namespace -> responseBuilder.addNamespaces( getNamespaceMeta( namespace ) ) );
        return responseBuilder.build();
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


    public static synchronized PrimaryKeysResponse getPrimaryKeys( String namespacePattern, String tablePattern ) {
        List<CatalogPrimaryKeyColumn> primaryKeyColumns = getPrimaryKeyColumns( namespacePattern, tablePattern );
        PrimaryKeysResponse.Builder responseBuilder = PrimaryKeysResponse.newBuilder();
        primaryKeyColumns.forEach( primaryKeyColumn -> responseBuilder.addPrimaryKeys( getPrimaryKeyColumnMeta( primaryKeyColumn ) ) );
        return responseBuilder.build();
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


    public static synchronized ImportedKeysResponse getImportedKeys( String namespacePattern, String tablePattern ) {
        List<CatalogForeignKeyColumn> foreignKeyColumns = getForeignKeyColumns( namespacePattern, tablePattern );
        ImportedKeysResponse.Builder responseBuilder = ImportedKeysResponse.newBuilder();
        foreignKeyColumns.forEach( foreignKeyColumn -> responseBuilder.addImportedKeys( getForeignKeyColumnMeta( foreignKeyColumn ) ) );
        return responseBuilder.build();
    }


    public static synchronized ExportedKeysResponse getExportedKeys( String schemaPattern, String tablePattern ) {
        List<CatalogForeignKeyColumn> exportedKeyColumns = getExportedKeyColumns( schemaPattern, tablePattern );
        ExportedKeysResponse.Builder responseBuilder = ExportedKeysResponse.newBuilder();
        exportedKeyColumns.forEach( foreignKeyColumn -> responseBuilder.addExportedKeys( getForeignKeyColumnMeta( foreignKeyColumn ) ) );
        return responseBuilder.build();
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
        Optional.ofNullable( typeSystem.getLiteral( polyType, true ) ).ifPresent( typeBuilder::setLiteralPrefix );
        Optional.ofNullable( typeSystem.getLiteral( polyType, false ) ).ifPresent( typeBuilder::setLiteralSuffix );
        typeBuilder.setIsCaseSensitive( typeSystem.isCaseSensitive( polyType ) );
        typeBuilder.setIsSearchable( DatabaseMetaData.typeSearchable );
        typeBuilder.setIsAutoIncrement( typeSystem.isAutoincrement( polyType ) );
        typeBuilder.setMinScale( polyType.getMinScale() );
        typeBuilder.setMaxScale( typeSystem.getMaxScale( polyType ) );
        typeBuilder.setRadix( typeSystem.getNumTypeRadix( polyType ) );
        return typeBuilder.build();
    }


    public static synchronized IndexesResponse getIndexes( String namespacePattern, String tablePattern, boolean unique ) {
        List<CatalogIndexColumn> catalogIndexColumns = getCatalogIndexColumns( namespacePattern, tablePattern, unique );
        IndexesResponse.Builder responseBuilder = IndexesResponse.newBuilder();
        catalogIndexColumns.forEach( indexColumn -> responseBuilder.addIndexes( getIndexColumnMeta( indexColumn ) ) );
        return responseBuilder.build();
    }


    private static Index getIndexColumnMeta( CatalogIndexColumn catalogIndexColumn ) {
        Serializable[] parameters = catalogIndexColumn.getParameterArray();
        Index.Builder importedKeyBuilder = Index.newBuilder();
        importedKeyBuilder.setDatabaseName( parameters[0].toString() );
        importedKeyBuilder.setNamespaceName( parameters[1].toString() );
        importedKeyBuilder.setTableName( parameters[2].toString() );
        importedKeyBuilder.setUnique( !Boolean.parseBoolean( parameters[3].toString() ) );
        importedKeyBuilder.setIndexName( parameters[5].toString() );
        importedKeyBuilder.setPositionIndex( Integer.parseInt( parameters[7].toString() ) );
        importedKeyBuilder.setColumnName( parameters[8].toString() );
        importedKeyBuilder.setLocation( parameters[13].toString() );
        importedKeyBuilder.setIndexType( Integer.parseInt( parameters[14].toString() ) );
        return importedKeyBuilder.build();
    }


    public static String getSqlKeywords() {
        // TODO: get data after functionality is implemented
        return "";
    }

    public static ProceduresResponse getProcedures(String languageName, String schemaPattern, String procedureNamePattern) {
        // TODO: get data after functionality is implemented
        return ProceduresResponse.newBuilder().build();
    }

    public static ProcedureColumnsResponse getProcedureColumns(String language, String namespacePattern, String procedurePattern, String columnPattern) {
        // TODO: get data after functionality is implemented
        return ProcedureColumnsResponse.newBuilder().build();
    }

/*


    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getDatabaseProperties( ConnectionHandle {} )", ch );
            }

            final Map<DatabaseProperty, Object> map = new HashMap<>();
            // TODO

            log.error( "[NOT IMPLEMENTED YET] getDatabaseProperties( ConnectionHandle {} )", ch );
            return map;
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