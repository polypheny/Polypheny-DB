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

package org.polypheny.db.adapter.neo4j;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.GraphDeleteTargets;
import org.polypheny.db.adapter.neo4j.Neo4jPlugin.Neo4jStore;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgScan;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;
import org.polypheny.db.adapter.neo4j.types.NestedSingleType;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.types.ModifiableGraph;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;


/**
 * Graph entity in the Neo4j representation.
 */
public class NeoGraph extends PhysicalGraph implements TranslatableEntity, ModifiableTable, ModifiableGraph, QueryableEntity {

    public final TransactionProvider transactionProvider;
    public final Driver db;
    public final String mappingLabel;
    public final Neo4jStore store;

    public List<? extends PhysicalField> fields;


    public NeoGraph( PhysicalEntity physical, List<? extends PhysicalField> fields, TransactionProvider transactionProvider, Driver db, String mappingLabel, Neo4jStore store ) {
        super( physical.id, physical.allocationId, physical.logicalId, physical.name, physical.adapterId );
        this.transactionProvider = transactionProvider;
        this.db = db;
        this.mappingLabel = mappingLabel;
        this.store = store;
        this.fields = fields;
    }


    /**
     * Creates an {@link RelModify} algebra object, which is modifies this graph.
     *
     * @param child the child nodes of the created algebra node
     * @param operation the modify operation
     */
    @Override
    public Modify<?> toModificationTable(
            AlgCluster cluster,
            AlgTraitSet traits,
            Entity table,
            AlgNode child,
            Operation operation,
            List<String> targets,
            List<? extends RexNode> sources ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalRelModify(
                cluster,
                traits.replace( Convention.NONE ),
                table,
                child,
                operation,
                targets,
                sources,
                false );
    }


    @Override
    public Modify<?> toModificationGraph(
            AlgCluster cluster,
            AlgTraitSet traits,
            Entity graph,
            AlgNode child,
            Operation operation,
            List<PolyString> targets,
            List<? extends RexNode> sources ) {
        NeoConvention.INSTANCE.register( cluster.getPlanner() );
        return new LogicalLpgModify(
                cluster,
                traits.replace( Convention.NONE ),
                graph,
                child,
                operation,
                targets,
                sources );
    }


    @Override
    public AlgNode toGraphRowModification( AlgCluster cluster, Operation operation, List<PolyType> parameterTypes, boolean detachDelete ) {
        return new NeoGraphRowModification( cluster, this, operation, parameterTypes.size(), detachDelete );
    }


    private static final class NeoGraphRowModification extends AbstractAlgNode implements EnumerableAlg {

        private final NeoGraph graph;
        private final Operation operation;
        private final int parameterCount;
        private final boolean detachDelete;


        private NeoGraphRowModification( AlgCluster cluster, NeoGraph graph, Operation operation, int parameterCount, boolean detachDelete ) {
            super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ).replace( ModelTrait.GRAPH ) );
            this.graph = graph;
            this.operation = operation;
            this.parameterCount = parameterCount;
            this.detachDelete = detachDelete;
            this.rowType = AlgOptUtil.createDmlRowType( Kind.INSERT, cluster.getTypeFactory() );
        }


        @Override
        public AlgWriter explainTerms( AlgWriter writer ) {
            return super.explainTerms( writer ).item( "graph", graph.id ).item( "operation", operation ).item( "parameters", parameterCount )
                    .itemIf( "detachDelete", true, detachDelete );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            Expression execute = Expressions.call( graph.asExpression(), "modifyRow", Expressions.constant( operation ),
                    Expressions.constant( parameterCount ), Expressions.constant( detachDelete ) );
            return implementor.result( PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.ARRAY, false ),
                    Blocks.toBlock( execute ) );
        }

    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new NeoLpgScan( cluster, cluster.traitSetOf( NeoConvention.INSTANCE ).replace( ModelTrait.GRAPH ), this );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call(
                Expressions.convert_(
                        Expressions.call(
                                Expressions.call(
                                        store.getCatalogAsExpression(),
                                        "getPhysical", Expressions.constant( id ) ),
                                "unwrapOrThrow", Expressions.constant( NeoGraph.class ) ),
                        NeoGraph.class ),
                "asQueryable",
                DataContext.ROOT,
                Catalog.SNAPSHOT_EXPRESSION );
    }


    @Override
    public NeoQueryable asQueryable( DataContext dataContext, Snapshot snapshot ) {
        return new NeoQueryable( dataContext, snapshot, this );
    }


    public static class NeoQueryable extends AbstractQueryable<PolyValue[]> {


        private final NeoGraph graph;
        private final DataContext dataContext;


        public NeoQueryable( DataContext dataContext, Snapshot snapshot, NeoGraph graph ) {
            this.dataContext = dataContext;
            this.graph = graph;
        }


        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> execute( String query, NestedPolyType types, Map<Long, Pair<PolyType, PolyType>> prepared ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( graph.store );

            List<Result> results = new ArrayList<>();
            results.add( trx.run( query ) );

            Function1<Record, PolyValue[]> getter = NeoQueryable.getter( types, graph.mappingLabel );

            return new AbstractEnumerable<>() {
                @Override
                public Enumerator<PolyValue[]> enumerator() {
                    return new NeoEnumerator( results, getter );
                }
            };
        }


        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> modifyRow( Operation operation, int parameterCount, boolean detachDelete ) {
            Transaction trx = getTrx();
            dataContext.getStatement().getTransaction().registerInvolvedAdapter( graph.store );
            if ( operation == Operation.DELETE ) {
                return deleteRows( trx, parameterCount, detachDelete );
            }
            Map<Long, PolyValue> parameters = dataContext.getParameterValues().get( 0 );
            List<GraphPropertyHolder> elements = new ArrayList<>();
            for ( int i = 0; i < parameterCount; i++ ) {
                PolyValue value = parameters.get( (long) i );
                if ( value != null && !value.isNull() ) {
                    elements.add( (GraphPropertyHolder) value );
                }
            }
            for ( GraphPropertyHolder element : elements ) {
                modifyElement( trx, operation, element );
            }
            return Functions.singletonEnumerable( new PolyValue[]{ PolyLong.of( elements.size() ) } );
        }


        private Enumerable<PolyValue[]> deleteRows( Transaction trx, int parameterCount, boolean detachDelete ) {
            GraphDeleteTargets targets = GraphDeleteTargets.collect( dataContext.getParameterValues(), parameterCount );
            String graphLabel = identifier( graph.mappingLabel );
            String matchNodes = "UNWIND $ids AS id MATCH (n:" + graphLabel + ") WHERE n._id IN [id, [id]]";
            List<String> nodes = targets.nodes().stream().map( PolyString::getValue ).toList();
            List<String> edges = targets.edges().stream().map( PolyString::getValue ).toList();
            if ( !detachDelete && !nodes.isEmpty() && trx.run( matchNodes
                            + " MATCH (n)-[r]-() WHERE none(edgeId IN $edges WHERE coalesce(r._id IN [edgeId, [edgeId]], false)) RETURN 1 LIMIT 1",
                    Map.of( "ids", nodes, "edges", edges ) ).hasNext() ) {
                throw new GenericRuntimeException( "Cannot delete a node with relationships. Delete its relationships or use DETACH DELETE." );
            }
            if ( !edges.isEmpty() ) {
                trx.run( "UNWIND $ids AS id MATCH (:" + graphLabel + ")-[r]->(:" + graphLabel + ") WHERE r._id IN [id, [id]] DELETE r",
                        Map.of( "ids", edges ) ).consume();
            }
            if ( !nodes.isEmpty() ) {
                trx.run( matchNodes + (detachDelete ? " DETACH DELETE n" : " DELETE n"), Map.of( "ids", nodes ) ).consume();
            }
            return Functions.singletonEnumerable( new PolyValue[]{ PolyLong.of( targets.rowCount() ) } );
        }


        private void modifyElement( Transaction trx, Operation operation, GraphPropertyHolder element ) {
            String graphLabel = identifier( graph.mappingLabel );
            boolean isNode = element.isNode();
            String match = (isNode ? "MATCH (n:" + graphLabel + ")"
                    : "MATCH (:" + graphLabel + ")-[n]->(:" + graphLabel + ")") + " WHERE n._id IN [$id, [$id]]";
            Map<String, Object> params = new LinkedHashMap<>();
            params.put( "id", element.id.value );
            Map<String, Object> properties = new LinkedHashMap<>();
            element.properties.forEach( (key, value) -> {
                if ( value != null && !value.isNull() ) {
                    PolyValue encoded = NeoUtil.encodeGraphProperty( value );
                    properties.put( NeoUtil.graphStorageKey( key.value ), NeoUtil.fixParameterValue( encoded, new NestedSingleType( encoded.getType() ), false ) );
                }
            } );
            properties.put( "_id", NeoUtil.hasEscapedGraphKeys( element.properties ) ? List.of( element.id.value ) : element.id.value );
            params.put( "properties", properties );
            if ( isNode ) {
                applyNodeWrite( trx, operation, element.asNode(), graphLabel, match, params );
            } else {
                applyEdgeWrite( trx, operation, element.asEdge(), graphLabel, match, params, properties );
            }
        }


        private void applyNodeWrite( Transaction trx, Operation operation, PolyNode node, String graphLabel, String match, Map<String, Object> params ) {
            String labels = node.labels.stream().map( label -> ":" + identifier( label.value ) ).collect( Collectors.joining() );
            if ( operation == Operation.INSERT ) {
                trx.run( "CREATE (n:" + graphLabel + labels + ") SET n = $properties", params ).consume();
                return;
            }
            List<Record> current = trx.run( match + " RETURN labels(n)", params ).list();
            if ( current.isEmpty() ) {
                throw new GenericRuntimeException( "Graph node disappeared before the update" );
            }
            String previous = current.get( 0 ).get( 0 ).asList( org.neo4j.driver.Value::asString ).stream()
                    .filter( label -> !label.equals( graph.mappingLabel ) ).map( label -> ":" + identifier( label ) ).collect( Collectors.joining() );
            trx.run( match + (previous.isEmpty() ? "" : " REMOVE n" + previous)
                    + (labels.isEmpty() ? "" : " SET n" + labels) + " SET n = $properties", params ).consume();
        }


        private void applyEdgeWrite( Transaction trx, Operation operation, PolyEdge edge, String graphLabel, String match,
                Map<String, Object> params, Map<String, Object> properties ) {
            String relationship = edge.labels.get( 0 ).value;
            properties.put( "__sourceId__", edge.left.value );
            properties.put( "__targetId__", edge.right.value );
            if ( operation == Operation.UPDATE ) {
                List<Record> current = trx.run( match + " RETURN type(n)", params ).list();
                if ( current.isEmpty() ) {
                    throw new GenericRuntimeException( "Graph relationship disappeared before the update" );
                }
                if ( relationship.equals( current.get( 0 ).get( 0 ).asString() ) ) {
                    trx.run( match + " SET n = $properties", params ).consume();
                    return;
                }
                trx.run( match + " DELETE n", params ).consume();
            }
            params.put( "left", edge.left.value );
            params.put( "right", edge.right.value );
            String endpoints = "MATCH (a:" + graphLabel + "), (b:" + graphLabel + ")"
                    + " WHERE a._id IN [$left, [$left]] AND b._id IN [$right, [$right]]";
            int written = trx.run( endpoints + " CREATE (a)-[n:" + identifier( relationship ) + "]->(b) SET n = $properties", params )
                    .consume().counters().relationshipsCreated();
            if ( written != 1 ) {
                throw new GenericRuntimeException( "Graph relationship requires exactly one source and one target node" );
            }
        }


        private static String identifier( String identifier ) {
            return "`" + identifier.replace( "`", "``" ) + "`";
        }


        /**
         * Creates a cypher statement which returns all edges and nodes separately
         *
         * @param nodes cypher statement to retrieve all nodes
         * @param edges cypher statement to retrieve all edges
         * @return the result as enumerable
         */
        @SuppressWarnings("UnusedDeclaration")
        public Enumerable<PolyValue[]> executeAll( String nodes, String edges ) {
            Transaction trx = getTrx();

            dataContext.getStatement().getTransaction().registerInvolvedAdapter( graph.store );

            Map<PolyString, PolyNode> polyNodes = trx.run( nodes ).list().stream().map( n -> NeoUtil.asPolyNode( n.get( 0 ).asNode(), graph.mappingLabel ) ).collect( Collectors.toMap( n -> n.id, n -> n ) );
            Map<PolyString, PolyEdge> polyEdges = trx.run( edges ).list().stream().map( e -> NeoUtil.asPolyEdge( e.get( 0 ).asRelationship() ) ).collect( Collectors.toMap( e -> e.id, e -> e ) );

            return Functions.singletonEnumerable( new PolyValue[]{ new PolyGraph( PolyMap.of( polyNodes ), PolyMap.of( polyEdges ) ) } );
        }


        /**
         * This method returns the functions, which transforms a given record into the corresponding object representation.
         *
         * @param types the types for which function is created
         * @return the function, which transforms the {@link Record}
         */
        static <T> Function1<Record, T> getter( NestedPolyType types, String mappingLabel ) {
            //noinspection unchecked
            return (Function1<Record, T>) NeoUtil.getTypesFunction( types, mappingLabel );
        }


        /**
         * Gets an already open transaction of creates a new one if it not exists.
         */
        private Transaction getTrx() {
            return graph.transactionProvider.get( dataContext.getStatement().getTransaction().getXid() );
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
        public @NotNull Iterator<PolyValue[]> iterator() {
            return Linq4j.enumeratorIterator( enumerator() );
        }


        @Override
        public Enumerator<PolyValue[]> enumerator() {
            return null;
        }

    }


}
