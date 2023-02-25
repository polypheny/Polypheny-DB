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

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgScan;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.refactor.ModifiableEntity;
import org.polypheny.db.catalog.refactor.TranslatableEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.QueryableGraph;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


/**
 * Graph entity in the Neo4j representation.
 */
public class NeoGraph extends PhysicalGraph implements TranslatableEntity, ModifiableEntity {

    public final TransactionProvider transactionProvider;
    public final Driver db;
    public final String mappingLabel;
    public final Neo4jStore store;
    public final PhysicalGraph allocation;


    public NeoGraph( PhysicalGraph graph, TransactionProvider transactionProvider, Driver db, String mappingLabel, Neo4jStore store ) {
        super( graph.id, graph.name, graph.entityType, graph.namespaceType );
        this.allocation = graph;
        this.transactionProvider = transactionProvider;
        this.db = db;
        this.mappingLabel = mappingLabel;
        this.store = store;
    }


    /**
     * Creates an {@link RelModify} algebra object, which is modifies this graph.
     *
     * @param cluster
     * @param child the child nodes of the created algebra node
     * @param operation the modify operation
     */
    @Override
    public LpgModify<CatalogEntity> toModificationAlg(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            CatalogEntity physicalEntity,
            AlgNode child,
            Operation operation,
            List<String> targets,
            List<RexNode> sources ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalLpgModify(
                cluster,
                traits.replace( Convention.NONE ),
                physicalEntity,
                child,
                operation,
                targets,
                sources );
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet ) {
        final AlgOptCluster cluster = context.getCluster();
        return new NeoLpgScan( cluster, cluster.traitSetOf( NeoConvention.INSTANCE ).replace( ModelTrait.GRAPH ), this );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    public static class NeoQueryable<T> extends AbstractQueryable<T> {


        private final NeoGraph graph;
        private final DataContext dataContext;


        public NeoQueryable( DataContext dataContext, QueryableGraph graph ) {
            this.dataContext = dataContext;
            this.graph = (NeoGraph) graph;
        }


        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<T> execute( String query, List<PolyType> types, List<PolyType> componentTypes, Map<Long, Pair<PolyType, PolyType>> prepared ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( graph.store );

            List<Result> results = new ArrayList<>();
            results.add( trx.run( query ) );

            Function1<Record, T> getter = NeoQueryable.getter( types, componentTypes );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<T> enumerator() {
                    return new NeoEnumerator<>( results, getter );
                }
            };
        }


        /**
         * Creates a cypher statement which returns all edges and nodes separately
         *
         * @param nodes cypher statement to retrieve all nodes
         * @param edges cypher statement to retrieve all edges
         * @return the result as enumerable
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<T> executeAll( String nodes, String edges ) {
            commit();
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( graph.store );

            Map<String, PolyNode> polyNodes = trx.run( nodes ).list().stream().map( n -> NeoUtil.asPolyNode( n.get( 0 ).asNode() ) ).collect( Collectors.toMap( n -> n.id, n -> n ) );
            Map<String, PolyEdge> polyEdges = trx.run( edges ).list().stream().map( e -> NeoUtil.asPolyEdge( e.get( 0 ).asRelationship() ) ).collect( Collectors.toMap( e -> e.id, e -> e ) );

            //noinspection unchecked
            return (Enumerable<T>) Linq4j.singletonEnumerable( new PolyGraph( PolyMap.of( polyNodes ), PolyMap.of( polyEdges ) ) );
        }


        /**
         * This method returns the functions, which transforms a given record into the corresponding object representation.
         *
         * @param types the types for which function is created
         * @param componentTypes component types for collection types.
         * @return the function, which transforms the {@link Record}
         */
        static <T> Function1<Record, T> getter( List<PolyType> types, List<PolyType> componentTypes ) {
            //noinspection unchecked
            return (Function1<Record, T>) NeoUtil.getTypesFunction( types, componentTypes );
        }


        /**
         * Gets an already open transaction of creates a new one if it not exists.
         */
        private Transaction getTrx() {
            return graph.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
        }


        /**
         * Commit the transaction.
         */
        private void commit() {
            graph.transactionProvider.commit( dataContext.getStatement().getTransaction().getXid() );
        }


        @Override
        public Type getElementType() {
            return Object[].class;
        }


        @Override
        public Expression getExpression() {
            return null;
        }


        @Override
        public QueryProvider getProvider() {
            return dataContext.getQueryProvider();
        }


        @Override
        public Iterator<T> iterator() {
            return Linq4j.enumeratorIterator( enumerator() );
        }


        @Override
        public Enumerator<T> enumerator() {
            return null;
        }

    }


    @Override
    public AlgDataType getRowType() {
        return null;
    }

}
