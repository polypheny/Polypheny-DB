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

import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.entity.logical.LogicalIndex.CatalogIndexColumn;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.protointerface.proto.*;
import org.polypheny.db.type.PolyType;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DbMetaRetriever {

    // Namespace search by name and type
    public static NamespacesResponse searchNamespaces(String namespacePattern, String namespaceType) {
        List<LogicalNamespace> namespaces = getLogicalNamespaces(namespacePattern, namespaceType);
        NamespacesResponse.Builder responseBuilder = NamespacesResponse.newBuilder();
        namespaces.forEach(namespace -> responseBuilder.addNamespaces(getNamespaceMeta(namespace)));
        return responseBuilder.build();
    }


    private static List<LogicalNamespace> getLogicalNamespaces(String namespacePattern, String namespaceType) {
        Pattern catalogNamespacePattern = getPatternOrNull(namespacePattern);
        List<LogicalNamespace> logicalNamespaces = Catalog.getInstance().getSnapshot().getNamespaces(catalogNamespacePattern);
        if (namespaceType == null) {
            return logicalNamespaces;
        }
        NamespaceType catalogNamespaceType = NamespaceType.valueOf(namespaceType);
        return logicalNamespaces.stream().filter(n -> n.getNamespaceType() == catalogNamespaceType).collect(Collectors.toList());
    }

    private static Namespace getNamespaceMeta(LogicalNamespace logicalNamespace) {
        Namespace.Builder namespaceBuilder = Namespace.newBuilder();
        namespaceBuilder.setNamespaceName(logicalNamespace.getName());
        namespaceBuilder.setDatabaseName(logicalNamespace.getDatabaseName());
        namespaceBuilder.setOwnerName(logicalNamespace.getOwnerName());
        Optional.ofNullable(logicalNamespace.getNamespaceType()).ifPresent(p -> namespaceBuilder.setNamespaceType(p.name()));
        return namespaceBuilder.build();
    }


    private static Pattern getPatternOrNull(String pattern) {
        return pattern == null ? null : new Pattern(pattern);
    }

    // Entity search by namespace
    public static EntityResponse searchEntities(String namespaceName, String entityPattern) {
        EntityResponse.Builder responseBuilder = EntityResponse.newBuilder();
        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace(namespaceName);
        switch (namespace.getNamespaceType()) {
            case RELATIONAL:
                responseBuilder.addAllEntities(getRelationalEntities(namespace.getId(), namespaceName));
                break;
            case GRAPH:
                responseBuilder.addAllEntities(getGraphEntities(namespace.getId(), namespaceName));
                break;
            case DOCUMENT:
                responseBuilder.addAllEntities(getDocumentEntities(namespace.getId(), entityPattern));
                break;
        }
        return responseBuilder.build();
    }

    // Relational entities
    private static List<Entity> getRelationalEntities(long namespaceId, String entityPattern) {
        Pattern catalogEntityPattern = getPatternOrNull(entityPattern);
        final List<LogicalTable> tables = Catalog.getInstance().getSnapshot().rel().getTables(namespaceId, catalogEntityPattern);
        return tables.stream().map(logicalTable -> buildEntityFromTable(getTableMeta(logicalTable))).collect(Collectors.toList());
    }

    private static Entity buildEntityFromTable(Table table) {
        return Entity.newBuilder()
                .setTable(table)
                .build();
    }

    private static Table getTableMeta(LogicalTable logicalTable) {
        return Table.newBuilder()
                .setSourceDatabaseName(logicalTable.getDatabaseName())
                .setNamespaceName(logicalTable.getNamespaceName())
                .setTableName(logicalTable.getName())
                .setTableType(logicalTable.getEntityType().name())
                .setOwnerName(logicalTable.getOwnerName())
                .addAllColumns(getColumns(logicalTable))
                .setPrimaryKey(getPrimaryKeyMeta(logicalTable))
                .addAllForeignKeys(getForeignKeys(logicalTable))
                .addAllIndexes(getIndexes(logicalTable, true))
                .build();
    }

    private static List<Column> getColumns(LogicalTable logicalTable) {
        return logicalTable.getColumns().stream().map(DbMetaRetriever::getColumnMeta).collect(Collectors.toList());
    }


    private static PrimaryKey getPrimaryKeyMeta(LogicalTable logicalTable) {
        LogicalPrimaryKey logicalPrimaryKey = Catalog.getInstance().getSnapshot().rel().getPrimaryKey(logicalTable.primaryKey).orElseThrow();
        return PrimaryKey.newBuilder()
                .setDatabaseName(logicalPrimaryKey.getDatabaseName())
                .setNamespaceName(logicalPrimaryKey.getSchemaName())
                .setTableName(logicalPrimaryKey.getTableName())
                .addAllColumns(getColumns(logicalPrimaryKey))
                .build();
    }

    private static List<Column> getColumns(LogicalKey logicalKey) {
        return logicalKey.getColumns().stream().map(DbMetaRetriever::getColumnMeta).collect(Collectors.toList());
    }

    private static Column getColumnMeta(LogicalColumn logicalColumn) {
        Column.Builder columnBuilder = Column.newBuilder();
        columnBuilder.setDatabaseName(logicalColumn.getDatabaseName());
        columnBuilder.setNamespaceName(logicalColumn.getNamespaceName());
        columnBuilder.setTableName(logicalColumn.getTableName());
        columnBuilder.setColumnName(logicalColumn.getName());
        columnBuilder.setTypeName(logicalColumn.getType().getTypeName());
        Optional.ofNullable(logicalColumn.getLength()).ifPresent(columnBuilder::setTypeLength);
        Optional.ofNullable(logicalColumn.getScale()).ifPresent(columnBuilder::setTypeScale);
        columnBuilder.setIsNullable(logicalColumn.isNullable());
        Optional.ofNullable(logicalColumn.getDefaultValue()).ifPresent(p -> columnBuilder.setDefaultValueAsString(p.getValue()));
        columnBuilder.setColumnIndex(logicalColumn.getPosition());
        Optional.ofNullable(CatalogObject.getEnumNameOrNull(logicalColumn.getCollation())).ifPresent(columnBuilder::setCollation);
        return columnBuilder.build();
    }


    private static List<ForeignKey> getForeignKeys(LogicalTable logicalTable) {
        return Catalog.getInstance().getSnapshot().rel().getForeignKeys(logicalTable.getId())
                .stream().map(DbMetaRetriever::getForeignKeyMeta).collect(Collectors.toList());
    }

    private static ForeignKey getForeignKeyMeta(LogicalForeignKey logicalForeignKey) {
        ForeignKey.Builder foreignKeyBuilder = ForeignKey.newBuilder();
        foreignKeyBuilder.setReferencedNamespaceName(logicalForeignKey.getReferencedKeySchemaName());
        foreignKeyBuilder.setReferencedDatabaseName(logicalForeignKey.getReferencedKeyDatabaseName());
        foreignKeyBuilder.setReferencedTableName(logicalForeignKey.getReferencedKeyTableName());
        foreignKeyBuilder.setUpdateRule(logicalForeignKey.getUpdateRule().getId());
        foreignKeyBuilder.setDeleteRule(logicalForeignKey.getDeleteRule().getId());
        Optional.ofNullable(logicalForeignKey.getName()).ifPresent(foreignKeyBuilder::setKeyName);
        foreignKeyBuilder.addAllReferencedColumns(getReferencedColumns(logicalForeignKey));
        foreignKeyBuilder.addAllForeignColumns(getColumns(logicalForeignKey));
        return foreignKeyBuilder.build();
    }

    private static List<Column> getReferencedColumns(LogicalForeignKey logicalForeignKey) {
        return logicalForeignKey.getReferencedColumns().stream().map(DbMetaRetriever::getColumnMeta).collect(Collectors.toList());
    }

    private static List<Index> getIndexes(LogicalTable logicalTable, boolean unique) {
        return Catalog.getInstance().getSnapshot().rel().getIndexes(logicalTable.getId(), unique)
                .stream().map(DbMetaRetriever::getIndexMeta).collect(Collectors.toList());
    }

    private static Index getIndexMeta(LogicalIndex logicalIndex) {
        Index.Builder importedKeyBuilder = Index.newBuilder();
        importedKeyBuilder.setDatabaseName(logicalIndex.getDatabaseName());
        importedKeyBuilder.setNamespaceName(logicalIndex.getKey().getSchemaName());
        importedKeyBuilder.setTableName(logicalIndex.getKey().getTableName());
        importedKeyBuilder.setUnique(logicalIndex.isUnique());
        importedKeyBuilder.setIndexName(logicalIndex.getName());
        importedKeyBuilder.addAllColumns(getColumns(logicalIndex.getKey()));
        importedKeyBuilder.setLocation(logicalIndex.getLocation());
        importedKeyBuilder.setIndexType(logicalIndex.getType().getId());
        return importedKeyBuilder.build();
    }


    public static List<Entity> getDocumentEntities(long namespaceId, String entityPattern) {
        return null;
    }

    public static List<Entity> getGraphEntities(long namespaceId, String entityPattern) {
        return null;
    }


    private static List<LogicalIndex> getLogicalIndexesOf(long entityId, boolean unique) {
        return Catalog.getInstance().getSnapshot().rel().getIndexes(entityId, unique);
    }


    public static synchronized DatabasesResponse getDatabases() {
        Database database = Database.newBuilder()
                .setDatabaseName(Catalog.DATABASE_NAME)
                .setOwnerName("system")
                .setDefaultNamespaceName(Catalog.defaultNamespaceName)
                .build();
        return DatabasesResponse.newBuilder()
                .addDatabases(database)
                .build();
    }


    public static synchronized TableTypesResponse getTableTypes() {
        List<String> tableTypes = Arrays.stream(EntityType.values()).map(EntityType::name).collect(Collectors.toList());
        TableTypesResponse.Builder responseBuilder = TableTypesResponse.newBuilder();
        tableTypes.forEach(tableType -> responseBuilder.addTableTypes(getTableTypeMeta(tableType)));
        return responseBuilder.build();
    }


    private static TableType getTableTypeMeta(String tableType) {
        return TableType.newBuilder().setTableType(tableType).build();
    }


    public static synchronized TypesResponse getTypes() {
        TypesResponse.Builder responseBuilder = TypesResponse.newBuilder();
        Arrays.stream(PolyType.values()).forEach(polyType -> responseBuilder.addTypes(getTypeMeta(polyType)));
        return responseBuilder.build();
    }


    private static Type getTypeMeta(PolyType polyType) {
        AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
        Type.Builder typeBuilder = Type.newBuilder();
        typeBuilder.setTypeName(polyType.getName());
        typeBuilder.setPrecision(typeSystem.getMaxPrecision(polyType));
        Optional.ofNullable(typeSystem.getLiteral(polyType, true)).ifPresent(typeBuilder::setLiteralPrefix);
        Optional.ofNullable(typeSystem.getLiteral(polyType, false)).ifPresent(typeBuilder::setLiteralSuffix);
        typeBuilder.setIsCaseSensitive(typeSystem.isCaseSensitive(polyType));
        typeBuilder.setIsSearchable(DatabaseMetaData.typeSearchable);
        typeBuilder.setIsAutoIncrement(typeSystem.isAutoincrement(polyType));
        typeBuilder.setMinScale(polyType.getMinScale());
        typeBuilder.setMaxScale(typeSystem.getMaxScale(polyType));
        typeBuilder.setRadix(typeSystem.getNumTypeRadix(polyType));
        return typeBuilder.build();
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