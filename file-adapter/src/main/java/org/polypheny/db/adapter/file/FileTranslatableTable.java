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


import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.rel.FileTableScan;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptTable.ToRelContext;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableModify.Operation;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelProtoDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.type.PolyType;


public class FileTranslatableTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    final File rootDir;
    @Getter
    private final String tableName;
    long tableId;
    @Getter
    List<String> columnNames;
    @Getter
    final Map<String, Long> columnIdMap;
    @Getter
    final Map<String, PolyType> columnTypeMap;
    /**
     * Ids of the columns that are part of the primary key
     */
    @Getter
    final List<Long> pkIds;
    @Getter
    FileStore store;
    @Getter
    FileSchema fileSchema;
    RelProtoDataType protoRowType;


    protected FileTranslatableTable( final FileSchema fileSchema,
            final String tableName,
            final long tableId,
            final List<Long> columnIds,
            final ArrayList<PolyType> columnTypes,
            final List<String> columnNames,
            final List<Long> pkIds,
            final RelProtoDataType protoRowType ) {
        super( Object.class );
        this.rootDir = fileSchema.getStore().getRootDir();
        this.tableName = tableName;
        this.tableId = tableId;
        this.store = fileSchema.getStore();
        this.fileSchema = fileSchema;
        this.pkIds = pkIds;
        this.protoRowType = protoRowType;

        this.columnNames = columnNames;
        this.columnIdMap = new HashMap<>();
        this.columnTypeMap = new HashMap<>();
        int i = 0;
        for ( String columnName : columnNames ) {
            this.columnIdMap.put( columnName, columnIds.get( i ) );
            this.columnTypeMap.put( columnName, columnTypes.get( i ) );
            i++;
        }
    }


    @Override
    public RelNode toRel( ToRelContext context, RelOptTable relOptTable ) {
        fileSchema.getConvention().register( context.getCluster().getPlanner() );
        return new FileTableScan( context.getCluster(), relOptTable, this );
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new UnsupportedOperationException( "getModifiableCollection not implemented" );
        //return new ArrayList<>();
    }


    @Override
    public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader, RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        fileSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify( cluster, cluster.traitSetOf( Convention.NONE ), table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        throw new UnsupportedOperationException();
        //System.out.println("as Queryable");
        //fileSchema.getConvention().register( dataContext.getStatement().getQueryProcessor().getPlanner() );
        //return new FileQueryable<>( dataContext, schema, this, tableName );
    }


    public class FileQueryable<T> extends AbstractTableQueryable<T> {

        public FileQueryable( DataContext dataContext, SchemaPlus schema, FileTranslatableTable table, String tableName ) {
            super( dataContext, schema, FileTranslatableTable.this, tableName );
        }


        @Override
        public Enumerator<T> enumerator() {
            throw new RuntimeException( "FileQueryable enumerator not yet implemented" );
        }

    }

}
