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

package org.polypheny.db.adapter.cottontail;

import java.lang.reflect.Type;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.cottontail.algebra.CottontailScan;
import org.polypheny.db.adapter.cottontail.enumberable.CottontailQueryEnumerable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
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

@EqualsAndHashCode(callSuper = true)
@Value
public class CottontailEntity extends PhysicalTable implements TranslatableEntity, ModifiableTable, QueryableEntity {

    CottontailStore store;
    @Getter
    CottontailNamespace cottontailNamespace;

    @Getter
    EntityName entityName;

    @Getter
    String physicalSchemaName;
    @Getter
    String physicalTableName;
    List<String> physicalColumnNames;


    public CottontailEntity(
            CottontailNamespace cottontailNamespace,
            String physicalSchemaName,
            PhysicalTable physical,
            CottontailStore cottontailStore ) {
        super(
                physical.id,
                physical.allocationId,
                physical.logicalId,
                physical.name,
                physical.columns,
                physical.namespaceId,
                physical.namespaceName,
                physical.uniqueFieldIds,
                physical.adapterId );

        this.store = cottontailStore;
        this.cottontailNamespace = cottontailNamespace;

        this.physicalSchemaName = physicalSchemaName;
        this.physicalTableName = physical.name;
        this.physicalColumnNames = physical.getColumnNames();

        this.entityName = EntityName.newBuilder()
                .setName( this.physicalTableName )
                .setSchema( SchemaName.newBuilder().setName( physicalSchemaName ).build() )
                .build();
    }

    @Override
    public String toString() {
        return "CottontailTable {" + physicalTableName + "}";
    }


    @Override
    public Modify<?> toModificationTable(
            AlgCluster cluster,
            AlgTraitSet algTraits,
            Entity table,
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
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new CottontailScan( cluster, this, this.cottontailNamespace.getConvention() );
    }


    public CottontailConvention getUnderlyingConvention() {
        return this.cottontailNamespace.getConvention();
    }


    @SuppressWarnings("unused")
    public void registerStore( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( this.store );
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
                    new CottontailQueryEnumerable.RowTypeParser( cottontailTable.getTupleType(), cottontailTable.physicalColumnNames )
            ).enumerator();
        }

    }

}
