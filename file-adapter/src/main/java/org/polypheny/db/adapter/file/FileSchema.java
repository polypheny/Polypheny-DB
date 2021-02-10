/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.file;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;


public class FileSchema extends AbstractSchema {

    @Getter
    private final String schemaName;
    private final Map<String, FileTranslatableTable> tableMap = new HashMap<>();
    @Getter
    private final FileStore store;
    @Getter
    private final FileConvention convention;


    public FileSchema( SchemaPlus parentSchema, String schemaName, FileStore store ) {
        super();
        this.schemaName = schemaName;
        this.store = store;
        final Expression expression = Schemas.subSchemaExpression( parentSchema, schemaName, FileSchema.class );
        this.convention = new FileConvention( schemaName, expression, this );
    }


    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }


    public Table createFileTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        ArrayList<Long> columnIds = new ArrayList<>();
        ArrayList<PolyType> columnTypes = new ArrayList<>();
        ArrayList<String> columnNames = new ArrayList<>();
        columnPlacementsOnStore.sort( Comparator.comparingLong( p -> p.columnId ) );
        for ( CatalogColumnPlacement p : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn;
            catalogColumn = Catalog.getInstance().getColumn( p.columnId );
            if ( p.adapterId == store.getAdapterId() ) {
                columnIds.add( p.columnId );
                if ( catalogColumn.collectionsType != null ) {
                    columnTypes.add( PolyType.ARRAY );
                } else {
                    columnTypes.add( catalogColumn.type );
                }
                columnNames.add( catalogColumn.name );

                if ( catalogColumn.type.allowsScale() && catalogColumn.length != null && catalogColumn.scale != null ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length, catalogColumn.scale ).nullable( catalogColumn.nullable );
                } else if ( catalogColumn.type.allowsPrec() && catalogColumn.length != null ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length ).nullable( catalogColumn.nullable );
                } else {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type ).nullable( catalogColumn.nullable );
                }
            }
        }
        RelProtoDataType protoRowType = RelDataTypeImpl.proto( fieldInfo.build() );
        List<Long> pkIds;
        if ( catalogTable.primaryKey != null ) {
            CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
            pkIds = primaryKey.columnIds;
        } else {
            pkIds = new ArrayList<>();
        }
        //FileTable table = new FileTable( store.getRootDir(), schemaName, catalogTable.id, columnIds, columnTypes, columnNames, store, this );
        FileTranslatableTable table = new FileTranslatableTable( this, catalogTable.name, catalogTable.id, columnIds, columnTypes, columnNames, pkIds, protoRowType );
        tableMap.put( catalogTable.name, table );
        return table;
    }


    /**
     * Called from generated code
     * Executes SELECT, UPDATE and DELETE operations
     * see {@link FileMethod#EXECUTE} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     */
    public static Enumerable<Object> execute( final Operation operation, final Integer storeId, final DataContext dataContext, final String path, final Long[] columnIds, final PolyType[] columnTypes, final List<Long> pkIds, final Integer[] projectionMapping, final Condition condition, final Value[] updates ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( storeId ) );
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new FileEnumerator( operation, path, columnIds, columnTypes, pkIds, projectionMapping, dataContext, condition, updates );
            }
        };
    }


    /**
     * Called from generated code
     * Executes INSERT operations
     * see {@link FileMethod#EXECUTE_MODIFY} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     */
    public static Enumerable<Object> executeModify( final Operation operation, final Integer storeId, final DataContext dataContext, final String path, final Long[] columnIds, final PolyType[] columnTypes, final List<Long> pkIds, final Boolean isBatch, final Object[] insertValues, final Condition condition ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( storeId ) );
        final Object[] insert;

        ArrayList<Object[]> rows = new ArrayList<>();
        ArrayList<Object> row = new ArrayList<>();
        int i = 0;
        if ( dataContext.getParameterValues().size() > 0 ) {
            for ( Map<Long, Object> map : dataContext.getParameterValues() ) {
                row.clear();
                //insertValues[] has length 1 if the dataContext is set
                for ( Value values : ((Value[]) insertValues[0]) ) {
                    row.add( values.getValue( dataContext, i ) );
                }
                rows.add( row.toArray( new Object[0] ) );
                i++;
            }
        } else {
            for ( Object insertRow : insertValues ) {
                row.clear();
                Value[] values = (Value[]) insertRow;
                for ( Value value : values ) {
                    row.add( value.getValue( dataContext, i ) );
                }
                rows.add( row.toArray( new Object[0] ) );
                i++;
            }
        }
        insert = rows.toArray( new Object[0] );

        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return new FileModifier( operation, path, columnIds, columnTypes, pkIds, dataContext, insert, condition );
            }
        };
    }

}
