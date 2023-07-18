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

import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
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

    public static Namespace getNamespace(String namespaceName) {
        return getNamespaceMeta(Catalog.getInstance().getSnapshot().getNamespace(namespaceName));
    }

    // Entity search by namespace
    public static EntitiesResponse searchEntities(String namespaceName, String entityPattern) {
        EntitiesResponse.Builder responseBuilder = EntitiesResponse.newBuilder();
        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace(namespaceName);
        switch (namespace.getNamespaceType()) {
            case RELATIONAL:
                responseBuilder.addAllEntities(getRelationalEntities(namespace.getId(), entityPattern));
                break;
            case GRAPH:
                responseBuilder.addAllEntities(getGraphEntities(namespace.getId(), entityPattern));
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
        Table.Builder tableBuilder = Table.newBuilder();
        tableBuilder.setSourceDatabaseName(logicalTable.getDatabaseName());
        tableBuilder.setNamespaceName(logicalTable.getNamespaceName());
        tableBuilder.setTableName(logicalTable.getName());
        tableBuilder.setTableType(logicalTable.getEntityType().name());
        tableBuilder.setOwnerName(logicalTable.getOwnerName());
        tableBuilder.addAllColumns(getColumns(logicalTable));
        if (logicalTable.primaryKey != null) {
            tableBuilder.setPrimaryKey(getPrimaryKeyMeta(logicalTable));
        }
        tableBuilder.addAllForeignKeys(getForeignKeys(logicalTable));
        tableBuilder.addAllExportedKeys(getExportedKeys(logicalTable));
        tableBuilder.addAllIndexes(getIndexes(logicalTable, true));
        return tableBuilder.build();
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
        columnBuilder.setIsHidden(false); //TODO: get from flag in catalog
        columnBuilder.setColumnType(Column.ColumnType.UNSPECIFIED); //TODO: get from catalog
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

    private static List<ForeignKey> getExportedKeys(LogicalTable logicalTable) {
        return Catalog.getInstance().getSnapshot().rel().getExportedKeys(logicalTable.getId())
                .stream().map(DbMetaRetriever::getForeignKeyMeta).collect(Collectors.toList());
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

    public static ProceduresResponse getProcedures(String languageName, String procedureNamePattern) {
        // TODO: get data after functionality is implemented
        return ProceduresResponse.newBuilder().build();
    }

    public static ClientInfoPropertyMetaResponse getClientInfoProperties() {
        List<ClientInfoPropertyMeta> propertyInfoMetas = PIClientInfoProperties.DEFAULTS.stream()
                .map(DbMetaRetriever::getClientInfoPropertyMeta).collect(Collectors.toList());
        return ClientInfoPropertyMetaResponse.newBuilder()
                .addAllClientInfoPropertyMetas(propertyInfoMetas)
                .build();
    }

    private static ClientInfoPropertyMeta getClientInfoPropertyMeta(PIClientInfoProperties.ClientInfoPropertiesDefault clientInfoPropertiesDefault) {
        return ClientInfoPropertyMeta.newBuilder()
                .setKey(clientInfoPropertiesDefault.key)
                .setDefaultValue(clientInfoPropertiesDefault.default_value)
                .setMaxlength(clientInfoPropertiesDefault.maxlength)
                .setDescription(clientInfoPropertiesDefault.description)
                .build();
    }

    public static FunctionsResponse getFunctions(QueryLanguage language, FunctionCategory functionCategory) {
        List<Function> functions = OperatorRegistry.getMatchingOperators(language).values().stream()
                .filter(o -> o instanceof org.polypheny.db.nodes.Function)
                .map(org.polypheny.db.nodes.Function.class::cast)
                .filter(f -> f.getFunctionCategory() == functionCategory || functionCategory == null)
                .map(DbMetaRetriever::getFunctionMeta)
                .collect(Collectors.toList());
        return FunctionsResponse.newBuilder().addAllFunctions(functions).build();
    }

    private static Function getFunctionMeta(org.polypheny.db.nodes.Function function) {
        Function.Builder functionBuilder = Function.newBuilder();
        functionBuilder.setName(function.getName());
        functionBuilder.setSyntax(function.getAllowedSignatures());
        functionBuilder.setFunctionCategory(function.getFunctionCategory().name());
        functionBuilder.setIsTableFunction(function.getFunctionCategory().isTableFunction());
        return functionBuilder.build();
    }

    public static DbmsVersionResponse getDbmsVersion() {
        /*
        String versionName = PolyphenyDb.class.getPackage().getImplementationVersion();
        if (versionName == null) {
            throw new PIServiceException("Could not retrieve database version info");
        }
        int nextSeparatorIndex = versionName.indexOf('.');
        if (nextSeparatorIndex <= 0) {
            throw new PIServiceException("Could not parse database version info");
        }
        int majorVersion = Integer.parseInt(versionName.substring(0, nextSeparatorIndex));

        versionName = versionName.substring(nextSeparatorIndex + 1);
        nextSeparatorIndex = versionName.indexOf('.');
        if (nextSeparatorIndex <= 0) {
            throw new PIServiceException("Could not parse database version info");
        }
        int minorVersion = Integer.parseInt(versionName.substring(0, nextSeparatorIndex));

        DbmsVersionResponse dbmsVersionResponse = DbmsVersionResponse.newBuilder()
                .setDbmsName("Polypheny-DB")
                .setVersionName(PolyphenyDb.class.getPackage().getImplementationVersion())
                .setMajorVersion(majorVersion)
                .setMinorVersion(minorVersion)
                .build();
        return dbmsVersionResponse;
        */
        DbmsVersionResponse dbmsVersionResponse = DbmsVersionResponse.newBuilder()
                .setDbmsName("Polypheny-DB")
                .setVersionName("DUMMY VERSION NAME")
                .setMajorVersion(-1)
                .setMinorVersion(-1)
                .build();
        return dbmsVersionResponse;

    }
}