/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import java.sql.DatabaseMetaData;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.entity.PolyObject.Visibility;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;
import org.polypheny.prism.ClientInfoPropertyMeta;
import org.polypheny.prism.ClientInfoPropertyMetaResponse;
import org.polypheny.prism.Column;
import org.polypheny.prism.DbmsVersionResponse;
import org.polypheny.prism.DefaultNamespaceResponse;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.Entity;
import org.polypheny.prism.ForeignKey;
import org.polypheny.prism.Function;
import org.polypheny.prism.FunctionsResponse;
import org.polypheny.prism.Index;
import org.polypheny.prism.Namespace;
import org.polypheny.prism.NamespacesResponse;
import org.polypheny.prism.PrimaryKey;
import org.polypheny.prism.ProceduresResponse;
import org.polypheny.prism.Table;
import org.polypheny.prism.TableType;
import org.polypheny.prism.TableTypesResponse;
import org.polypheny.prism.Type;
import org.polypheny.prism.TypesResponse;

class DbMetaRetriever {

    // Namespace search by name and type
    static NamespacesResponse searchNamespaces( String namespacePattern, String namespaceType ) {
        List<LogicalNamespace> namespaces = getLogicalNamespaces( namespacePattern, namespaceType );
        NamespacesResponse.Builder responseBuilder = NamespacesResponse.newBuilder();
        namespaces.forEach( namespace -> responseBuilder.addNamespaces( getNamespaceMeta( namespace ) ) );
        return responseBuilder.build();
    }


    private static List<LogicalNamespace> getLogicalNamespaces( String namespacePattern, String namespaceType ) {
        Pattern catalogNamespacePattern = getPatternOrNull( namespacePattern );
        List<LogicalNamespace> logicalNamespaces = Catalog.getInstance().getSnapshot().getNamespaces( catalogNamespacePattern );
        if ( namespaceType == null ) {
            return logicalNamespaces;
        }
        DataModel catalogNamespaceType = DataModel.valueOf( namespaceType );
        return logicalNamespaces.stream().filter( n -> n.getDataModel() == catalogNamespaceType ).toList();
    }


    private static Namespace getNamespaceMeta( LogicalNamespace logicalNamespace ) {
        Namespace.Builder namespaceBuilder = Namespace.newBuilder();
        namespaceBuilder.setNamespaceName( logicalNamespace.getName() );
        Optional.ofNullable( logicalNamespace.getDataModel() ).ifPresent( p -> namespaceBuilder.setNamespaceType( p.name() ) );
        return namespaceBuilder.build();
    }


    private static Pattern getPatternOrNull( String pattern ) {
        return pattern == null ? null : new Pattern( pattern );
    }


    public static Namespace getNamespace( String namespaceName ) {
        return getNamespaceMeta( Catalog.getInstance().getSnapshot().getNamespace( namespaceName ).orElseThrow() );
    }


    // Entity search by namespace
    static EntitiesResponse searchEntities( String namespaceName, String entityPattern ) {
        EntitiesResponse.Builder responseBuilder = EntitiesResponse.newBuilder();
        LogicalNamespace namespace = Catalog.getInstance().getSnapshot().getNamespace( namespaceName ).orElseThrow();
        return switch ( namespace.getDataModel() ) {
            case RELATIONAL -> responseBuilder.addAllEntities( getRelationalEntities( namespace.getId(), entityPattern ) ).build();
            case GRAPH -> responseBuilder.addAllEntities( getGraphEntities( namespace.getId(), entityPattern ) ).build();
            case DOCUMENT -> responseBuilder.addAllEntities( getDocumentEntities( namespace.getId(), entityPattern ) ).build();
        };
    }


    // Relational entities
    private static List<Entity> getRelationalEntities( long namespaceId, String entityPattern ) {
        Pattern catalogEntityPattern = getPatternOrNull( entityPattern );
        final List<LogicalTable> tables = Catalog.getInstance().getSnapshot().rel().getTables( namespaceId, catalogEntityPattern );
        return tables.stream().map( logicalTable -> buildEntityFromTable( getTableMeta( logicalTable ) ) ).toList();
    }


    private static Entity buildEntityFromTable( Table table ) {
        return Entity.newBuilder()
                .setTable( table )
                .build();
    }


    private static Table getTableMeta( LogicalTable logicalTable ) {
        Table.Builder tableBuilder = Table.newBuilder();
        tableBuilder.setNamespaceName( logicalTable.getNamespaceName() );
        tableBuilder.setTableName( logicalTable.getName() );
        tableBuilder.setTableType( logicalTable.getEntityType().name() );
        tableBuilder.addAllColumns( getColumns( logicalTable ) );
        if ( hasPrimaryKey( logicalTable ) ) {
            tableBuilder.setPrimaryKey( getPrimaryKeyMeta( logicalTable ) );
        }
        tableBuilder.addAllForeignKeys( getForeignKeys( logicalTable ) );
        tableBuilder.addAllExportedKeys( getExportedKeys( logicalTable ) );
        tableBuilder.addAllIndexes( getIndexes( logicalTable, false ) );
        return tableBuilder.build();
    }


    private static List<Column> getColumns( LogicalTable logicalTable ) {
        return logicalTable.getColumns().stream().map( DbMetaRetriever::getColumnMeta ).toList();
    }


    private static boolean hasPrimaryKey( LogicalTable logicalTable ) {
        if ( logicalTable.primaryKey == null ) {
            return false;
        }
        return Catalog.getInstance().getSnapshot().rel().getPrimaryKey( logicalTable.primaryKey ).isPresent();
    }


    private static PrimaryKey getPrimaryKeyMeta( LogicalTable logicalTable ) {
        LogicalPrimaryKey logicalPrimaryKey = Catalog.getInstance().getSnapshot().rel().getPrimaryKey( logicalTable.primaryKey ).orElseThrow();
        return PrimaryKey.newBuilder()
                .setDatabaseName( logicalPrimaryKey.getSchemaName() )
                .setNamespaceName( logicalPrimaryKey.getSchemaName() )
                .setTableName( logicalPrimaryKey.getTableName() )
                .addAllColumns( getColumns( logicalPrimaryKey ) )
                .build();
    }


    private static List<Column> getColumns( LogicalKey logicalKey ) {
        return logicalKey.fieldIds.stream().map( id -> Catalog.snapshot().rel().getColumn( id ).orElseThrow() ).map( DbMetaRetriever::getColumnMeta ).toList();
    }


    private static Column getColumnMeta( LogicalColumn logicalColumn ) {
        Column.Builder columnBuilder = Column.newBuilder();
        columnBuilder.setNamespaceName( logicalColumn.getNamespaceName() );
        columnBuilder.setTableName( logicalColumn.getTableName() );
        columnBuilder.setColumnName( logicalColumn.getName() );
        columnBuilder.setTypeName( logicalColumn.getType().getTypeName() );
        Optional.ofNullable( logicalColumn.getLength() ).ifPresent( columnBuilder::setTypeLength );
        Optional.ofNullable( logicalColumn.getScale() ).ifPresent( columnBuilder::setTypeScale );
        columnBuilder.setIsNullable( logicalColumn.isNullable() );
        Optional.ofNullable( logicalColumn.getDefaultValue() ).ifPresent( p -> columnBuilder.setDefaultValueAsString( p.value.toJson() ) );
        columnBuilder.setColumnIndex( logicalColumn.getPosition() );
        Optional.ofNullable( PolyObject.getEnumNameOrNull( logicalColumn.getCollation() ) ).ifPresent( columnBuilder::setCollation );
        columnBuilder.setIsHidden( logicalColumn.getVisibility() == Visibility.INTERNAL );
        columnBuilder.setColumnType( Column.ColumnType.UNSPECIFIED ); //TODO: reserved for future use
        return columnBuilder.build();
    }


    private static List<ForeignKey> getForeignKeys( LogicalTable logicalTable ) {
        return Catalog.getInstance().getSnapshot().rel().getForeignKeys( logicalTable.getId() )
                .stream().map( DbMetaRetriever::getForeignKeyMeta ).toList();
    }


    private static ForeignKey getForeignKeyMeta( LogicalForeignKey logicalForeignKey ) {
        ForeignKey.Builder foreignKeyBuilder = ForeignKey.newBuilder();
        foreignKeyBuilder.setReferencedNamespaceName( logicalForeignKey.getReferencedKeyNamespaceName() );
        foreignKeyBuilder.setReferencedTableName( logicalForeignKey.getReferencedKeyEntityName() );
        foreignKeyBuilder.setUpdateRule( logicalForeignKey.getUpdateRule().getId() );
        foreignKeyBuilder.setDeleteRule( logicalForeignKey.getDeleteRule().getId() );
        Optional.ofNullable( logicalForeignKey.getName() ).ifPresent( foreignKeyBuilder::setKeyName );
        foreignKeyBuilder.addAllReferencedColumns( getReferencedColumns( logicalForeignKey ) );
        foreignKeyBuilder.addAllForeignColumns( getColumns( logicalForeignKey ) );
        return foreignKeyBuilder.build();
    }


    private static List<Column> getReferencedColumns( LogicalForeignKey logicalForeignKey ) {
        return logicalForeignKey.referencedKeyFieldIds.stream().map( id -> Catalog.snapshot().rel().getColumn( id ).orElseThrow() ).map( DbMetaRetriever::getColumnMeta ).toList();
    }


    private static List<ForeignKey> getExportedKeys( LogicalTable logicalTable ) {
        return Catalog.getInstance().getSnapshot().rel().getExportedKeys( logicalTable.getId() )
                .stream().map( DbMetaRetriever::getForeignKeyMeta ).toList();
    }


    private static List<Index> getIndexes( LogicalTable logicalTable, boolean unique ) {
        return Catalog.getInstance().getSnapshot().rel().getIndexes( logicalTable.getId(), unique )
                .stream().map( DbMetaRetriever::getIndexMeta ).toList();
    }


    private static Index getIndexMeta( LogicalIndex logicalIndex ) {
        Index.Builder importedKeyBuilder = Index.newBuilder();
        importedKeyBuilder.setNamespaceName( logicalIndex.getKey().getSchemaName() );
        importedKeyBuilder.setTableName( logicalIndex.getKey().getTableName() );
        importedKeyBuilder.setUnique( logicalIndex.isUnique() );
        importedKeyBuilder.setIndexName( logicalIndex.getName() );
        importedKeyBuilder.addAllColumns( getColumns( logicalIndex.getKey() ) );
        importedKeyBuilder.setLocation( logicalIndex.getLocation() );
        importedKeyBuilder.setIndexType( logicalIndex.getType().getId() );
        return importedKeyBuilder.build();
    }


    static List<Entity> getDocumentEntities( long namespaceId, String entityPattern ) {
        return null;
    }


    static List<Entity> getGraphEntities( long namespaceId, String entityPattern ) {
        return null;
    }


    static synchronized DefaultNamespaceResponse getDefaultNamespace() {
        return DefaultNamespaceResponse.newBuilder()
                .setDefaultNamespace( Catalog.DEFAULT_NAMESPACE_NAME )
                .build();
    }


    static synchronized TableTypesResponse getTableTypes() {
        List<String> tableTypes = Arrays.stream( EntityType.values() ).map( EntityType::name ).toList();
        TableTypesResponse.Builder responseBuilder = TableTypesResponse.newBuilder();
        tableTypes.forEach( tableType -> responseBuilder.addTableTypes( getTableTypeMeta( tableType ) ) );
        return responseBuilder.build();
    }


    private static TableType getTableTypeMeta( String tableType ) {
        return TableType.newBuilder().setTableType( tableType ).build();
    }


    static synchronized TypesResponse getTypes() {
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


    static ProceduresResponse getProcedures( String languageName, String procedureNamePattern ) {
        // TODO: get data after functionality is implemented
        return ProceduresResponse.newBuilder().build();
    }


    static ClientInfoPropertyMetaResponse getClientInfoProperties() {
        List<ClientInfoPropertyMeta> propertyInfoMetas = PIClientInfoProperties.DEFAULTS.stream()
                .map( DbMetaRetriever::getClientInfoPropertyMeta ).toList();
        return ClientInfoPropertyMetaResponse.newBuilder()
                .addAllClientInfoPropertyMetas( propertyInfoMetas )
                .build();
    }


    private static ClientInfoPropertyMeta getClientInfoPropertyMeta( PIClientInfoProperties.ClientInfoPropertiesDefault clientInfoPropertiesDefault ) {
        return ClientInfoPropertyMeta.newBuilder()
                .setKey( clientInfoPropertiesDefault.key() )
                .setDefaultValue( clientInfoPropertiesDefault.default_value() )
                .setMaxlength( clientInfoPropertiesDefault.maxLength() )
                .setDescription( clientInfoPropertiesDefault.description() )
                .build();
    }


    static FunctionsResponse getFunctions( QueryLanguage language, FunctionCategory functionCategory ) {
        List<Function> functions = OperatorRegistry.getMatchingOperators( language ).values().stream()
                .filter( o -> o instanceof org.polypheny.db.nodes.Function )
                .map( org.polypheny.db.nodes.Function.class::cast )
                .filter( f -> f.getFunctionCategory() == functionCategory || functionCategory == null )
                .map( DbMetaRetriever::getFunctionMeta )
                .toList();
        return FunctionsResponse.newBuilder().addAllFunctions( functions ).build();
    }


    private static Function getFunctionMeta( org.polypheny.db.nodes.Function function ) {
        Function.Builder functionBuilder = Function.newBuilder();
        functionBuilder.setName( function.getName() );
        functionBuilder.setSyntax( function.getAllowedSignatures() );
        functionBuilder.setFunctionCategory( function.getFunctionCategory().name() );
        functionBuilder.setIsTableFunction( function.getFunctionCategory().isTableFunction() );
        return functionBuilder.build();
    }


    static DbmsVersionResponse getDbmsVersion() {
        try {
            String versionName = PolyphenyDb.class.getPackage().getImplementationVersion();
            if ( versionName == null ) {
                throw new PIServiceException( "Could not retrieve database version info" );
            }
            int nextSeparatorIndex = versionName.indexOf( '.' );
            if ( nextSeparatorIndex <= 0 ) {
                throw new PIServiceException( "Could not parse database version info" );
            }
            int majorVersion = Integer.parseInt( versionName.substring( 0, nextSeparatorIndex ) );

            versionName = versionName.substring( nextSeparatorIndex + 1 );
            nextSeparatorIndex = versionName.indexOf( '.' );
            if ( nextSeparatorIndex <= 0 ) {
                throw new PIServiceException( "Could not parse database version info" );
            }
            int minorVersion = Integer.parseInt( versionName.substring( 0, nextSeparatorIndex ) );

            return DbmsVersionResponse.newBuilder()
                    .setDbmsName( "Polypheny-DB" )
                    .setVersionName( PolyphenyDb.class.getPackage().getImplementationVersion() )
                    .setMajorVersion( majorVersion )
                    .setMinorVersion( minorVersion )
                    .build();
        } catch ( Exception e ) {
            return DbmsVersionResponse.newBuilder()
                    .setDbmsName( "Polypheny-DB" )
                    .setVersionName( "DEVELOPMENT VERSION" )
                    .setMajorVersion( -1 )
                    .setMinorVersion( -1 )
                    .build();
        }
    }

}
