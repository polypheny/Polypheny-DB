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

package org.polypheny.db.adapter.cottontail;

import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.algebra.CottontailScan;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailQueryEnumerable;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Snapshot;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.refactor.ModifiableEntity;
import org.polypheny.db.catalog.refactor.QueryableEntity;
import org.polypheny.db.catalog.refactor.TranslatableEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Metadata;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Scan;
import org.vitrivr.cottontail.grpc.CottontailGrpc.SchemaName;


public class CottontailEntity extends PhysicalTable implements TranslatableEntity, ModifiableEntity, QueryableEntity {

    private final LogicalTable logical;
    private final AllocationTable allocation;
    private AlgProtoDataType protoRowType;
    private CottontailSchema cottontailSchema;

    @Getter
    private EntityName entityName;

    @Getter
    private final String physicalSchemaName;
    @Getter
    private final String physicalTableName;
    private final List<String> physicalColumnNames;

    private final List<String> logicalColumnNames;


    protected CottontailEntity(
            CottontailSchema cottontailSchema,
            String physicalSchemaName,
            LogicalTable logical,
            AllocationTable allocation ) {
        super( allocation );

        this.cottontailSchema = cottontailSchema;

        this.logicalColumnNames = logical.getColumnNames();
        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = CottontailNameUtil.createPhysicalTableName( logical.id, allocation.id );
        this.physicalColumnNames = allocation.placements.stream().map( p -> CottontailNameUtil.createPhysicalColumnName( p.columnId ) ).collect( Collectors.toList() );

        this.logical = logical;
        this.allocation = allocation;

        this.entityName = EntityName.newBuilder()
                .setName( this.physicalTableName )
                .setSchema( SchemaName.newBuilder().setName( physicalSchemaName ).build() )
                .build();
    }


    public String getPhysicalColumnName( String logicalColumnName ) {
        return this.physicalColumnNames.get( this.logicalColumnNames.indexOf( logicalColumnName ) );
    }


    @Override
    public String toString() {
        return "CottontailTable {" + physicalSchemaName + "." + physicalTableName + "}";
    }


    @Override
    public Modify<?> toModificationAlg(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            CatalogEntity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList
    ) {
        this.cottontailSchema.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster.traitSetOf( Convention.NONE ),
                table,
                input,
                operation,
                updateColumnList,
                sourceExpressionList );
    }


    @Override
    public Queryable<Object[]> asQueryable( DataContext dataContext, Snapshot snapshot, long entityId ) {
        return new CottontailTableQueryable( dataContext, snapshot, this );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet ) {
        return new CottontailScan( context.getCluster(), this, traitSet, this.cottontailSchema.getConvention() );
    }


    public CottontailConvention getUnderlyingConvention() {
        return this.cottontailSchema.getConvention();
    }


    @Override
    public Type getElementType() {
        return Object[].class;
    }


    private static class CottontailTableQueryable extends AbstractTableQueryable<Object[], CottontailEntity> {

        public CottontailTableQueryable( DataContext dataContext, Snapshot snapshot, CottontailEntity physicalTable ) {
            super( dataContext, snapshot, physicalTable );
        }


        @Override
        public Enumerator enumerator() {
            final JavaTypeFactory typeFactory = dataContext.getTypeFactory();
            final CottontailEntity cottontailTable = (CottontailEntity) this.table;
            final long txId = cottontailTable.cottontailSchema.getWrapper().beginOrContinue( this.dataContext.getStatement().getTransaction() );
            final Query query = Query.newBuilder()
                    .setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( cottontailTable.entityName ) ).build() )
                    .build();
            final QueryMessage queryMessage = QueryMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setQuery( query )
                    .build();
            return new CottontailQueryEnumerable(
                    cottontailTable.cottontailSchema.getWrapper().query( queryMessage ),
                    new CottontailQueryEnumerable.RowTypeParser( cottontailTable.getRowType(), cottontailTable.physicalColumnNames )
            ).enumerator();
        }

    }

}
