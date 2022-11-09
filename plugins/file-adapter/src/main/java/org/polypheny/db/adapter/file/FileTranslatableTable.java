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
import org.polypheny.db.adapter.file.algebra.FileScan;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.type.PolyType;


public class FileTranslatableTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {

    private final File rootDir;
    @Getter
    private final String tableName;
    @Getter
    private final long partitionId;
    @Getter
    private final List<String> columnNames;
    @Getter
    private final Map<String, Long> columnIdMap;
    @Getter
    private final Map<String, PolyType> columnTypeMap;
    @Getter
    private final List<Long> pkIds; // Ids of the columns that are part of the primary key
    @Getter
    private final int adapterId;
    @Getter
    private final FileSchema fileSchema;
    private final AlgProtoDataType protoRowType;


    public FileTranslatableTable(
            final FileSchema fileSchema,
            final String tableName,
            final Long tableId,
            final long partitionId,
            final List<Long> columnIds,
            final ArrayList<PolyType> columnTypes,
            final List<String> columnNames,
            final List<Long> pkIds,
            final AlgProtoDataType protoRowType ) {
        super( Object[].class );
        this.fileSchema = fileSchema;
        this.rootDir = fileSchema.getRootDir();
        this.tableName = tableName;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.adapterId = fileSchema.getAdapterId();
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
    public AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable ) {
        fileSchema.getConvention().register( context.getCluster().getPlanner() );
        return new FileScan( context.getCluster(), algOptTable, this );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    @Override
    public Collection getModifiableCollection() {
        throw new UnsupportedOperationException( "getModifiableCollection not implemented" );
        //return new ArrayList<>();
    }


    @Override
    public Modify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptTable table,
            CatalogReader catalogReader,
            AlgNode child,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        fileSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                child,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
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
