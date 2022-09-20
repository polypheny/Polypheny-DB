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

package org.polypheny.db.adapter.neo4j;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgScan;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.TranslatableGraph;
import org.polypheny.db.schema.graph.Graph;
import org.polypheny.db.schema.graph.ModifiableGraph;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.QueryableGraph;
import org.polypheny.db.schema.impl.AbstractSchema;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


/**
 * Graph entity in the Neo4j representation.
 */
public class NeoGraph extends AbstractSchema implements ModifiableGraph, TranslatableGraph, QueryableGraph {

    public final String name;
    public final TransactionProvider transactionProvider;
    public final Driver db;
    @Getter
    private final long id;
    public final String mappingLabel;
    public final Neo4jStore store;


    public NeoGraph( String name, TransactionProvider transactionProvider, Driver db, long id, String mappingLabel, Neo4jStore store ) {
        this.name = name;
        this.id = id;
        this.transactionProvider = transactionProvider;
        this.db = db;
        this.mappingLabel = mappingLabel;
        this.store = store;
    }


    /**
     * Creates an {@link org.polypheny.db.algebra.core.Modify} algebra object, which is modifies this graph.
     *
     * @param graph the {@link org.polypheny.db.schema.PolyphenyDbSchema} graph object
     * @param input the child nodes of the created algebra node
     * @param operation the modify operation
     * @param ids the ids, which are modified by the created algebra opertions
     * @param operations the operations to perform
     */
    @Override
    public LpgModify toModificationAlg(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            Graph graph,
            PolyphenyDbCatalogReader catalogReader,
            AlgNode input,
            Operation operation,
            List<String> ids, List<? extends RexNode> operations ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalLpgModify(
                cluster,
                traits.replace( Convention.NONE ),
                graph,
                input,
                operation,
                ids,
                operations );
    }


    @Override
    public Expression getExpression( SchemaPlus schema, String tableName, Class<?> clazz ) {
        return null;
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return null;
    }


    @Override
    public Statistic getStatistic() {
        return null;
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        return null;
    }


    @Override
    public AlgNode toAlg( ToAlgContext context, Graph graph ) {
        final AlgOptCluster cluster = context.getCluster();
        return new NeoLpgScan( cluster, cluster.traitSetOf( NeoConvention.INSTANCE ).replace( ModelTrait.GRAPH ), this );
    }


    @Override
    public <T> Queryable<T> asQueryable( DataContext root, QueryableGraph graph ) {
        return new NeoQueryable<>( root, this );
    }


    public static class NeoQueryable<T> extends AbstractQueryable<T> {


        private final NeoGraph graph;
        private final DataContext dataContext;


        public NeoQueryable( DataContext dataContext, Graph graph ) {
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

}
