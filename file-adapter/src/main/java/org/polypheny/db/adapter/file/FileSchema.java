/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
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
        for ( CatalogColumnPlacement p : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn;
            catalogColumn = Catalog.getInstance().getColumn( p.columnId );
            if ( p.storeId == store.getStoreId() ) {
                columnIds.add( p.columnId );
                //todo arrayType
                columnTypes.add( catalogColumn.type );
                columnNames.add( catalogColumn.name );
                //todo catalogColumn.defaultValue

                if ( catalogColumn.type.allowsScale() ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length, catalogColumn.scale ).nullable( catalogColumn.nullable );
                } else if ( catalogColumn.type.allowsPrec() ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length ).nullable( catalogColumn.nullable );
                } else {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type ).nullable( catalogColumn.nullable );
                }
            }
        }
        RelProtoDataType protoRowType = RelDataTypeImpl.proto( fieldInfo.build() );
        //FileTable table = new FileTable( store.getRootDir(), schemaName, catalogTable.id, columnIds, columnTypes, columnNames, store, this );
        FileTranslatableTable table = new FileTranslatableTable( this, catalogTable.name, catalogTable.id, columnIds, columnTypes, columnNames, protoRowType );
        tableMap.put( catalogTable.name, table );
        return table;
    }

    /**
     * Called from generated code
     * see {@link FileMethod#EXECUTE_SELECT} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     */
    public static Enumerable<Object[]> executeSelect( final DataContext dataContext, final String path, final Long[] columnIds, final PolyType[] columnTypes ) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new FileEnumerator<>( path, columnIds, columnTypes, dataContext.getStatement().getTransaction().getCancelFlag() );
            }
        };
    }

    /**
     * Called from generated code
     * see {@link FileMethod#EXECUTE_MODIFY} and {@link org.polypheny.db.adapter.file.rel.FileToEnumerableConverter#implement}
     */
    public static Enumerable<Object[]> executeModify( final DataContext dataContext, final String path, final Long[] columnIds, final PolyType[] columnTypes, final Boolean isBatch, final Object[] insertValues ) {
        final Object[] insert;
        if ( isBatch ) {
            ArrayList<Object[]> rows = new ArrayList<>();
            for ( Map<Long, Object> map : dataContext.getParameterValues() ) {
                ArrayList<Object> row = new ArrayList<>();
                for ( Long colId : columnIds ) {
                    row.add( map.get( colId ) );
                }
                rows.add( row.toArray( new Object[0] ) );
            }
            insert = rows.toArray( new Object[0] );
        } else {
            insert = insertValues;
        }
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new FileModifier<>( path, columnIds, columnTypes, dataContext.getStatement().getTransaction().getCancelFlag(), insert );
            }
        };
    }

}
