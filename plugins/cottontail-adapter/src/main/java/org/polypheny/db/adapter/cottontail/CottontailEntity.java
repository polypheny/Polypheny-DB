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
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.algebra.CottontailScan;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailQueryEnumerable;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractEntityQueryable;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Metadata;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Scan;
import org.vitrivr.cottontail.grpc.CottontailGrpc.SchemaName;


public class CottontailEntity extends PhysicalTable implements TranslatableEntity, ModifiableTable, QueryableEntity {

    private final PhysicalTable table;
    private AlgProtoDataType protoRowType;
    private CottontailNamespace cottontailNamespace;

    @Getter
    private EntityName entityName;

    @Getter
    private final String physicalSchemaName;
    @Getter
    private final String physicalTableName;
    private final List<String> physicalColumnNames;


    protected CottontailEntity(
            CottontailNamespace cottontailNamespace,
            String physicalSchemaName,
            PhysicalTable physical ) {
        super( physical.id,
                physical.allocationId,
                physical.logicalId,
                physical.name,
                physical.columns,
                physical.namespaceId,
                physical.namespaceName,
                physical.adapterId );

        this.cottontailNamespace = cottontailNamespace;
        this.table = physical;

        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physical.name;
        this.physicalColumnNames = physical.getColumnNames();

        this.entityName = EntityName.newBuilder()
                .setName( this.physicalTableName )
                .setSchema( SchemaName.newBuilder().setName( physicalSchemaName ).build() )
                .build();
    }


    public String getPhysicalColumnName( String logicalColumnName ) {
        return this.physicalColumnNames.get( this.table.columns.indexOf( logicalColumnName ) );
    }


    @Override
    public String toString() {
        return "CottontailTable {" + physicalSchemaName + "." + physicalTableName + "}";
    }


    @Override
    public Modify<?> toModificationTable(
            AlgOptCluster cluster,
            AlgTraitSet algTraits,
            CatalogEntity table,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList ) {
        this.cottontailNamespace.getConvention().register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster.traitSetOf( Convention.NONE ),
                table,
                input,
                operation,
                updateColumnList,
                sourceExpressionList );
    }


    @Override
    public Queryable<PolyValue[]> asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new CottontailTableQueryable( dataContext, snapshot, this );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet ) {
        return new CottontailScan( context.getCluster(), this, traitSet, this.cottontailNamespace.getConvention() );
    }


    public CottontailConvention getUnderlyingConvention() {
        return this.cottontailNamespace.getConvention();
    }


    @Override
    public Type getElementType() {
        return Object[].class;
    }


    private static class CottontailTableQueryable extends AbstractEntityQueryable<PolyValue[], CottontailEntity> {

        public CottontailTableQueryable( DataContext dataContext, Snapshot snapshot, CottontailEntity physicalTable ) {
            super( dataContext, snapshot, physicalTable );
        }


        @Override
        public Enumerator<PolyValue[]> enumerator() {
            final JavaTypeFactory typeFactory = dataContext.getTypeFactory();
            final CottontailEntity cottontailTable = entity;
            final long txId = cottontailTable.cottontailNamespace.getWrapper().beginOrContinue( this.dataContext.getStatement().getTransaction() );
            final Query query = Query.newBuilder()
                    .setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( cottontailTable.entityName ) ).build() )
                    .build();
            final QueryMessage queryMessage = QueryMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setQuery( query )
                    .build();
            return new CottontailQueryEnumerable(
                    cottontailTable.cottontailNamespace.getWrapper().query( queryMessage ),
                    new CottontailQueryEnumerable.RowTypeParser( cottontailTable.getRowType(), cottontailTable.physicalColumnNames )
            ).enumerator();
        }

    }

}
