/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.file.source;


import java.io.File;
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
import org.polypheny.db.adapter.file.Condition;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;


public class QfsSchema extends AbstractSchema implements FileSchema {

    @Getter
    private final String schemaName;
    private final Map<String, FileTranslatableTable> tableMap = new HashMap<>();
    @Getter
    private final Qfs source;
    @Getter
    private final FileConvention convention;


    public QfsSchema( SchemaPlus parentSchema, String schemaName, Qfs source ) {
        super();
        this.schemaName = schemaName;
        this.source = source;
        final Expression expression = Schemas.subSchemaExpression( parentSchema, schemaName, QfsSchema.class );
        this.convention = new QfsConvention( schemaName, expression, this );
    }


    @Override
    public File getRootDir() {
        return source.getRootDir();
    }


    @Override
    public int getAdapterId() {
        return source.getAdapterId();
    }


    @Override
    protected Map<String, Table> getTableMap() {
        return new HashMap<>( tableMap );
    }


    public Table createFileTable( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        ArrayList<Long> columnIds = new ArrayList<>();
        ArrayList<PolyType> columnTypes = new ArrayList<>();
        ArrayList<String> columnNames = new ArrayList<>();
        columnPlacementsOnStore.sort( Comparator.comparingLong( p -> p.columnId ) );
        for ( CatalogColumnPlacement p : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn;
            catalogColumn = Catalog.getInstance().getColumn( p.columnId );
            if ( p.adapterId == source.getAdapterId() ) {
                columnIds.add( p.columnId );
                if ( catalogColumn.collectionsType != null ) {
                    columnTypes.add( PolyType.ARRAY );
                } else {
                    columnTypes.add( catalogColumn.type );
                }
                columnNames.add( catalogColumn.name );

                if ( catalogColumn.type.allowsScale() && catalogColumn.length != null && catalogColumn.scale != null ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length, catalogColumn.scale )
                            .nullable( catalogColumn.nullable );
                } else if ( catalogColumn.type.allowsPrec() && catalogColumn.length != null ) {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type, catalogColumn.length )
                            .nullable( catalogColumn.nullable );
                } else {
                    fieldInfo.add( catalogColumn.name, p.physicalColumnName, catalogColumn.type )
                            .nullable( catalogColumn.nullable );
                }
            }
        }
        AlgProtoDataType protoRowType = AlgDataTypeImpl.proto( fieldInfo.build() );
        List<Long> pkIds;
        if ( catalogTable.primaryKey != null ) {
            CatalogPrimaryKey primaryKey = Catalog.getInstance().getPrimaryKey( catalogTable.primaryKey );
            pkIds = primaryKey.columnIds;
        } else {
            pkIds = new ArrayList<>();
        }
        FileTranslatableTable table = new FileTranslatableTable(
                this,
                catalogTable.name + "_" + partitionPlacement.partitionId,
                catalogTable.id,
                partitionPlacement.partitionId,
                columnIds,
                columnTypes,
                columnNames,
                pkIds,
                protoRowType );
        tableMap.put( catalogTable.name + "_" + partitionPlacement.partitionId, table );
        return table;
    }


    /**
     * Called from generated code
     */
    public static Enumerable<Object[]> execute(
            final Operation operation,
            final Integer adapterId,
            final Long partitionId,
            final DataContext dataContext,
            final String path,
            final Long[] columnIds,
            final PolyType[] columnTypes,
            final List<Long> pkIds,
            final Integer[] projectionMapping,
            final Condition condition,
            final Value[] updates ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getAdapter( adapterId ) );
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new QfsEnumerator<>( dataContext, path, columnIds, projectionMapping, condition );
            }
        };
    }

}
