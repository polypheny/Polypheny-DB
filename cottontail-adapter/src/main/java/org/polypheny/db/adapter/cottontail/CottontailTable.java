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

package org.polypheny.db.adapter.cottontail;


import java.util.Collection;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailQueryEnumerable;
import org.polypheny.db.adapter.cottontail.rel.CottontailTableScan;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.adapter.java.JavaTypeFactory;
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
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Schema;


public class CottontailTable extends AbstractQueryableTable implements TranslatableTable, ModifiableTable {  // implements TranslatableTable

    private RelProtoDataType protoRowType;
    private CottontailSchema cottontailSchema;

    @Getter
    private Entity entity;

    @Getter
    private final String physicalSchemaName;
    @Getter
    private final String physicalTableName;
    private final List<String> physicalColumnNames;

    private final String logicalSchemaName;
    private final String logicalTableName;
    private final List<String> logicalColumnNames;


    protected CottontailTable(
            CottontailSchema cottontailSchema,
            String logicalSchemaName,
            String logicalTableName,
            List<String> logicalColumnNames,
            RelProtoDataType protoRowType,
            String physicalSchemaName,
            String physicalTableName,
            List<String> physicalColumnNames ) {
        super( Object[].class );

        this.cottontailSchema = cottontailSchema;
        this.protoRowType = protoRowType;

        this.logicalSchemaName = logicalSchemaName;
        this.logicalTableName = logicalTableName;
        this.logicalColumnNames = logicalColumnNames;
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physicalTableName;
        this.physicalColumnNames = physicalColumnNames;

        this.entity = Entity.newBuilder().setName( this.physicalTableName ).setSchema( Schema.newBuilder().setName( physicalSchemaName ).build() ).build();
    }


    public String getPhysicalColumnName( String logicalColumnName ) {
        return this.physicalColumnNames.get( this.logicalColumnNames.indexOf( logicalColumnName ) );
    }


    @Override
    public String toString() {
        return "CottontailTable {" + physicalSchemaName + "." + physicalTableName + "}";
    }


    @Override
    public Collection getModifiableCollection() {
        return null;
    }


    @Override
    public TableModify toModificationRel(
            RelOptCluster cluster,
            RelOptTable table,
            CatalogReader catalogReader,
            RelNode input,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        this.cottontailSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalTableModify(
                cluster,
                cluster.traitSetOf( Convention.NONE ),
                table,
                catalogReader,
                input,
                operation,
                updateColumnList,
                sourceExpressionList,
                flattened );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new CottontailTableQueryable<>( dataContext, schema, tableName );
    }


    /*@Override
    public Enumerable<Object[]> scan( DataContext root ) {
        Query query = Query.newBuilder()
                .setFrom( From.newBuilder().setEntity( this.entity ).build() )
                .build();
        QueryMessage queryMessage = QueryMessage.newBuilder().setQuery( query ).build();


        return new CottontailQueryEnumerable<>(
                this.cottontailSchema.getWrapper().query( queryMessage ),
                new CottontailQueryEnumerable.RowTypeParser(
                        this.getRowType( root.getTypeFactory() ),
                        this.physicalColumnNames ) );
    }*/


    @Override
    public RelNode toRel( ToRelContext context, RelOptTable relOptTable ) {
        return new CottontailTableScan( context.getCluster(), relOptTable, this, this.cottontailSchema.getConvention() );
    }


    @Override
    public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
        return protoRowType.apply( typeFactory );
    }


    public CottontailConvention getUnderlyingConvention() {
        return this.cottontailSchema.getConvention();
    }


    private class CottontailTableQueryable<T> extends AbstractTableQueryable<T> {

        public CottontailTableQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
            super( dataContext, schema, CottontailTable.this, tableName );
        }


        @Override
        public Enumerator<T> enumerator() {
            final JavaTypeFactory typeFactory = dataContext.getTypeFactory();
            CottontailTable cottontailTable = (CottontailTable) this.table;
            Query query = Query.newBuilder()
                    .setFrom( From.newBuilder().setEntity( cottontailTable.entity ).build() )
                    .build();
            QueryMessage queryMessage = QueryMessage.newBuilder().setQuery( query ).build();
            final Enumerable enumerable = new CottontailQueryEnumerable<>(
                    cottontailTable.cottontailSchema.getWrapper().query( queryMessage ),
                    new CottontailQueryEnumerable.RowTypeParser(
                            cottontailTable.getRowType( typeFactory ),
                            cottontailTable.physicalColumnNames ) );
            return enumerable.enumerator();
        }

    }

}
