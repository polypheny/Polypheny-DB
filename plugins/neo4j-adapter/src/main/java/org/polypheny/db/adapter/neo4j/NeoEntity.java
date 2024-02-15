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

package org.polypheny.db.adapter.neo4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoScan;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractEntityQueryable;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

/**
 * Relational Neo4j representation of a {@link org.polypheny.db.schema.PolyphenyDbSchema} entity
 */
public class NeoEntity extends PhysicalEntity implements TranslatableEntity, ModifiableTable, QueryableEntity {

    @Getter
    private final List<? extends PhysicalField> fields;

    private final NeoNamespace namespace;


    protected NeoEntity( PhysicalEntity physical, List<? extends PhysicalField> fields, NeoNamespace namespace ) {
        super( physical.id, physical.allocationId, physical.logicalId, physical.name, physical.namespaceId, physical.namespaceName, physical.uniqueFieldIds, physical.dataModel, physical.adapterId );
        this.fields = fields;
        this.namespace = namespace;
    }


    @Override
    public AlgNode toAlg( AlgOptCluster cluster, AlgTraitSet traitSet ) {
        return new NeoScan( cluster, traitSet.replace( NeoConvention.INSTANCE ), this );
    }


    /**
     * Creates an {@link RelModify} algebra object, which is modifies this relational entity.
     *
     * @param child child algebra nodes of the created algebra operation
     * @param operation the operation type
     */
    @Override
    public Modify<Entity> toModificationTable(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            Entity physical,
            AlgNode child,
            Operation operation,
            List<String> targets,
            List<? extends RexNode> sources ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalRelModify(
                traits.replace( Convention.NONE ),
                physical,
                child,
                operation,
                targets,
                sources
        );
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call(
                Expressions.convert_(
                        Expressions.call(
                                Expressions.call(
                                        namespace.store.getCatalogAsExpression(),
                                        "getPhysical", Expressions.constant( id ) ),
                                "unwrapOrThrow", Expressions.constant( NeoEntity.class ) ),
                        NeoEntity.class ),
                "asQueryable",
                DataContext.ROOT,
                Catalog.SNAPSHOT_EXPRESSION );
    }


    @Override
    public NeoQueryable asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new NeoQueryable( dataContext, snapshot, this );
    }


    @Override
    public PhysicalEntity normalize() {
        return new PhysicalGraph( id, allocationId, logicalId, name, adapterId );
    }


    public AlgDataType getRowType() {
        if ( dataModel == DataModel.RELATIONAL ) {
            return buildProto().apply( AlgDataTypeFactory.DEFAULT );
        }
        return super.getRowType();
    }


    public AlgProtoDataType buildProto() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( PhysicalColumn column : fields.stream().map( f -> f.unwrap( PhysicalColumn.class ).orElseThrow() ).sorted( Comparator.comparingInt( a -> a.position ) ).toList() ) {
            AlgDataType sqlType = column.getAlgDataType( AlgDataTypeFactory.DEFAULT );
            fieldInfo.add( column.id, column.logicalName, column.name, sqlType ).nullable( column.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() );
    }


    public static class NeoQueryable extends AbstractEntityQueryable<PolyValue[], NeoEntity> {

        private final AlgDataType rowType;


        public NeoQueryable( DataContext dataContext, Snapshot snapshot, NeoEntity entity ) {
            super( dataContext, snapshot, entity );
            this.rowType = entity.getRowType();
        }


        @Override
        public Enumerator<PolyValue[]> enumerator() {
            return execute(
                    String.format( "MATCH (n:%s) RETURN %s", entity.name, buildAllQuery() ),
                    getTypes(),
                    getComponentType(),
                    Map.of() ).enumerator();
        }


        private List<PolyType> getComponentType() {
            return rowType.getFields().stream().map( t -> {
                if ( t.getType().getComponentType() != null ) {
                    return t.getType().getComponentType().getPolyType();
                }
                return null;
            } ).toList();
        }


        private List<PolyType> getTypes() {
            return rowType.getFields().stream().map( t -> t.getType().getPolyType() ).toList();
        }


        private String buildAllQuery() {
            return rowType.getFields().stream().map( f -> "n." + f.getPhysicalName() ).collect( Collectors.joining( ", " ) );
        }


        /**
         * Executes the given query and returns a {@link Enumerable}, which returns the results when iterated.
         *
         * @param query the query to execute
         * @param prepared mapping of parameters and their components if they are collections
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> execute( String query, List<PolyType> types, List<PolyType> componentTypes, Map<Long, Pair<PolyType, PolyType>> prepared ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( entity.namespace.store );

            List<Result> results = new ArrayList<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                results.add( trx.run( query, toParameters( dataContext.getParameterValues().get( 0 ), prepared ) ) );
            } else if ( !dataContext.getParameterValues().isEmpty() ) {
                for ( Map<Long, PolyValue> value : dataContext.getParameterValues() ) {
                    results.add( trx.run( query, toParameters( value, prepared ) ) );
                }
            } else {
                results.add( trx.run( query ) );
            }

            Function1<Record, PolyValue[]> getter = NeoQueryable.getter( types, componentTypes );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<PolyValue[]> enumerator() {
                    return new NeoEnumerator( results, getter );
                }
            };
        }


        /**
         * Creates a mapping of parameters to the provided values, as Neo4j needs it.
         *
         * @param values the values to execute the query with
         * @param parameterTypes the types of the attached values
         */
        private Map<String, Object> toParameters( Map<Long, PolyValue> values, Map<Long, Pair<PolyType, PolyType>> parameterTypes ) {
            Map<String, Object> parameters = new HashMap<>();
            for ( Entry<Long, PolyValue> entry : values.entrySet().stream().filter( e -> parameterTypes.containsKey( e.getKey() ) ).toList() ) {
                parameters.put(
                        NeoUtil.asParameter( entry.getKey(), false ),
                        NeoUtil.fixParameterValue( entry.getValue(), parameterTypes.get( entry.getKey() ) ) );
            }
            return parameters;
        }


        static Function1<Record, PolyValue[]> getter( List<PolyType> types, List<PolyType> componentTypes ) {
            return NeoUtil.getTypesFunction( types, componentTypes );
        }


        private Transaction getTrx() {
            return entity.namespace.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }

    }

}
