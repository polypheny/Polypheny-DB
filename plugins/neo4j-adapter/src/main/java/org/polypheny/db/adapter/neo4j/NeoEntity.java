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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableEntity;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoScan;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgProtoDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ModifiableEntity;
import org.polypheny.db.schema.QueryableEntity;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TranslatableEntity;
import org.polypheny.db.schema.impl.AbstractTableQueryable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Relational Neo4j representation of a {@link org.polypheny.db.schema.PolyphenyDbSchema} entity
 */
public class NeoEntity extends AbstractQueryableEntity implements TranslatableEntity, ModifiableEntity {

    public final String physicalEntityName;
    public final AlgProtoDataType rowType;


    protected NeoEntity( String physicalEntityName, AlgProtoDataType proto, long id, long partitionId, long adapterId ) {
        super( Object[].class, id, partitionId, adapterId );
        this.physicalEntityName = physicalEntityName;
        this.rowType = proto;
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext dataContext, SchemaPlus schema, String tableName ) {
        return new NeoQueryable<>( dataContext, schema, this, tableName );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return rowType.apply( getTypeFactory() );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgOptEntity algOptEntity, AlgTraitSet traitSet ) {
        final AlgOptCluster cluster = context.getCluster();
        return new NeoScan( cluster, traitSet.replace( NeoConvention.INSTANCE ), algOptEntity, this );
    }


    /**
     * Creates an {@link org.polypheny.db.algebra.core.Modify} algebra object, which is modifies this relational entity.
     *
     * @param child child algebra nodes of the created algebra operation
     * @param operation the operation type
     * @param updateColumnList the target elements of the modification
     * @param sourceExpressionList the modify operation to create the new values
     * @param flattened if the {@link Modify} is flattened
     */
    @Override
    public Modify toModificationAlg(
            AlgOptCluster cluster,
            AlgOptEntity table,
            CatalogReader catalogReader,
            AlgNode child,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            boolean flattened ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
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


    public static class NeoQueryable<T> extends AbstractTableQueryable<T> {

        @Getter
        private final NeoEntity entity;
        private final NeoSchema namespace;
        private final AlgDataType rowType;


        public NeoQueryable( DataContext dataContext, SchemaPlus schema, QueryableEntity table, String tableName ) {
            super( dataContext, schema, table, tableName );
            this.entity = (NeoEntity) table;
            this.namespace = schema.unwrap( NeoSchema.class );
            this.rowType = entity.rowType.apply( entity.getTypeFactory() );
        }


        @Override
        public Enumerator<T> enumerator() {
            return execute(
                    String.format( "MATCH (n:%s) RETURN %s", entity.physicalEntityName, buildAllQuery() ),
                    getTypes(),
                    getComponentType(),
                    Map.of() ).enumerator();
        }


        private List<PolyType> getComponentType() {
            return rowType.getFieldList().stream().map( t -> {
                if ( t.getType().getComponentType() != null ) {
                    return t.getType().getComponentType().getPolyType();
                }
                return null;
            } ).collect( Collectors.toList() );
        }


        private List<PolyType> getTypes() {
            return rowType.getFieldList().stream().map( t -> t.getType().getPolyType() ).collect( Collectors.toList() );
        }


        private String buildAllQuery() {
            return rowType.getFieldList().stream().map( f -> "n." + f.getPhysicalName() ).collect( Collectors.joining( ", " ) );
        }


        /**
         * Executes the given query and returns a {@link Enumerable}, which returns the results when iterated.
         *
         * @param query the query to execute
         * @param prepared mapping of parameters and their components if they are collections
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<T> execute( String query, List<PolyType> types, List<PolyType> componentTypes, Map<Long, Pair<PolyType, PolyType>> prepared ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( namespace.store );

            List<Result> results = new ArrayList<>();
            if ( dataContext.getParameterValues().size() == 1 ) {
                results.add( trx.run( query, toParameters( dataContext.getParameterValues().get( 0 ), prepared ) ) );
            } else if ( dataContext.getParameterValues().size() > 0 ) {
                for ( Map<Long, Object> value : dataContext.getParameterValues() ) {
                    results.add( trx.run( query, toParameters( value, prepared ) ) );
                }
            } else {
                results.add( trx.run( query ) );
            }

            Function1<Record, T> getter = NeoQueryable.getter( types, componentTypes );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<T> enumerator() {
                    return new NeoEnumerator<>( results, getter );
                }
            };
        }


        /**
         * Creates a mapping of parameters to the provided values, as Neo4j needs it.
         *
         * @param values the values to execute the query with
         * @param parameterTypes the types of the attached values
         */
        private Map<String, Object> toParameters( Map<Long, Object> values, Map<Long, Pair<PolyType, PolyType>> parameterTypes ) {
            Map<String, Object> parameters = new HashMap<>();
            for ( Entry<Long, Object> entry : values.entrySet().stream().filter( e -> parameterTypes.containsKey( e.getKey() ) ).collect( Collectors.toList() ) ) {
                parameters.put(
                        NeoUtil.asParameter( entry.getKey(), false ),
                        NeoUtil.fixParameterValue( entry.getValue(), parameterTypes.get( entry.getKey() ) ) );
            }
            return parameters;
        }


        static <T> Function1<Record, T> getter( List<PolyType> types, List<PolyType> componentTypes ) {
            //noinspection unchecked
            return (Function1<Record, T>) NeoUtil.getTypesFunction( types, componentTypes );
        }


        private Transaction getTrx() {
            return namespace.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }

    }

}
