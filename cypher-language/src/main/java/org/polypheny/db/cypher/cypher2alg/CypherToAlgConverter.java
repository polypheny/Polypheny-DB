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

package org.polypheny.db.cypher.cypher2alg;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.graph.LogicalGraph;
import org.polypheny.db.algebra.logical.graph.LogicalGraphFilter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphMatch;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownNamespaceException;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherNode.CypherFamily;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class CypherToAlgConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final AlgBuilder algBuilder;
    private final Statement statement;
    private final RexBuilder rexBuilder;
    private final AlgOptCluster cluster;


    public CypherToAlgConverter( Statement statement, AlgBuilder builder, RexBuilder rexBuilder, AlgOptCluster cluster ) {
        this.catalogReader = statement.getTransaction().getCatalogReader();
        this.statement = statement;
        this.algBuilder = builder;
        this.rexBuilder = rexBuilder;
        this.cluster = cluster;
    }


    public AlgRoot convert( CypherNode query, CypherQueryParameters parameters, AlgOptCluster cluster ) {
        long databaseId;
        if ( parameters.databaseId == null ) {
            databaseId = getDatabaseId( parameters );
        } else {
            databaseId = parameters.databaseId;
        }

        LogicalGraph graph = new LogicalGraph( databaseId );

        if ( !CypherFamily.QUERY.contains( query.getCypherKind() ) ) {
            throw new RuntimeException( "Used a unsupported query." );
        }

        CypherContext context = new CypherContext( query, graph, cluster, algBuilder, rexBuilder, catalogReader );

        convertQuery( query, context );


        return AlgRoot.of( context.build(), context.kind );
    }


    private long getDatabaseId( CypherQueryParameters parameters ) {
        long databaseId;
        try {
            databaseId = Catalog.getInstance().getNamespace( Catalog.defaultDatabaseId, parameters.databaseName ).id;
        } catch ( UnknownNamespaceException e ) {
            throw new RuntimeException( "Error on retrieving the used namespace" );
        }
        return databaseId;
    }


    private void convertQuery( CypherNode node, CypherContext context ) {
        switch ( node.getCypherKind() ) {
            case SINGLE:
                CypherSingleQuery query = (CypherSingleQuery) node;

                for ( CypherClause clause : query.getClauses() ) {
                    convertClauses( clause, context );
                }

                break;
            case PERIODIC_COMMIT:
            case UNION:
                throw new UnsupportedOperationException();
        }
    }


    private void convertClauses( CypherClause clause, CypherContext context ) {
        switch ( clause.getCypherKind() ) {
            case MATCH:
                convertMatch( (CypherMatch) clause, context );
                break;
            case RETURN:
                convertReturn( (CypherReturnClause) clause, context );
                break;
            case CREATE:
                convertCreate( (CypherCreate) clause, context );
                break;
            case USE:
                break;
            default:
                throw new UnsupportedOperationException();
        }

    }


    private void convertReturn( CypherReturnClause clause, CypherContext context ) {
        context.add( clause.getGraphProject( context ) );
    }


    private void convertCreate( CypherCreate clause, CypherContext context ) {
        context.kind = Kind.INSERT;

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.add( new LogicalGraphModify( cluster, cluster.traitSet(), context.graph, catalogReader, context.pop(), Operation.INSERT, null, null ) );
    }


    private void convertMatch( CypherMatch clause, CypherContext context ) {
        setKindIfNull( context, Kind.SELECT );
        if ( clause.isOptional() ) {
            throw new UnsupportedOperationException();
        }

        context.active = clause;

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.combineMatch();

        if ( clause.getWhere() == null ) {
            return;
        }

        convertWhere( clause.getWhere(), context );

        context.combineFilter();


    }


    private void convertWhere( CypherWhere where, CypherContext context ) {
        context.add( where.getExpression().getRexNode( context ) );
    }


    private void convertPattern( CypherPattern pattern, CypherContext context ) {
        if ( context.kind == Kind.INSERT ) {
            // convert "values" pattern (LogicalGraphValues AlgNode)
            context.add( pattern.getPatternValues( context ) );
        } else {
            // convert filter pattern ( RexNode CYPHER_PATTERN_MATCH )
            context.add( pattern.getPatternFilter( context ) );
        }

    }


    private void setKindIfNull( CypherContext context, Kind kind ) {
        if ( context.kind == null ) {
            context.kind = kind;
        }
    }


    public static class CypherContext {

        public final AlgOptCluster cluster;
        public final AlgBuilder algBuilder;
        public final RexBuilder rexBuilder;

        private final Stack<AlgNode> stack = new Stack<>();
        private final Stack<RexNode> rexStack = new Stack<>();
        public final CypherNode original;
        public final LogicalGraph graph;

        public final AlgDataType graphType;
        public final AlgDataType booleanType;
        public final AlgDataType nodeType;
        public final AlgDataType edgeType;
        private final PolyphenyDbCatalogReader catalogReader;
        public CypherNode active;
        public Kind kind;


        private CypherContext( CypherNode original, LogicalGraph graph, AlgOptCluster cluster, AlgBuilder algBuilder, RexBuilder rexBuilder, PolyphenyDbCatalogReader catalogReader ) {
            this.original = original;
            this.graph = graph;
            this.cluster = cluster;
            this.algBuilder = algBuilder;
            this.rexBuilder = rexBuilder;
            this.graphType = cluster.getTypeFactory().createPolyType( PolyType.GRAPH );
            this.booleanType = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
            this.nodeType = cluster.getTypeFactory().createPolyType( PolyType.NODE );
            this.edgeType = cluster.getTypeFactory().createPolyType( PolyType.EDGE );
            this.catalogReader = catalogReader;
        }


        public void addDefaultScanIfNecessary() {
            if ( !stack.isEmpty() ) {
                return;
            }
            stack.add( new LogicalGraphScan( cluster, catalogReader, cluster.traitSet(), graph, new AlgRecordType(
                    List.of(
                            new AlgDataTypeFieldImpl( "n", 0, nodeType ),
                            new AlgDataTypeFieldImpl( "e", 1, edgeType ) ) ) ) );

        }


        public void combineMatch() {
            addDefaultScanIfNecessary();
            RexNode condition = getCondition();
            if ( condition.isAlwaysTrue() ) {
                // blank MATCH (n) without condition
                return;
            }

            AlgNode node = stack.pop();

            stack.add( new LogicalGraphMatch( cluster, cluster.traitSet(), node, condition ) );

        }


        public void combineFilter() {
            addDefaultScanIfNecessary();
            AlgNode node = stack.pop();
            RexNode condition = getCondition();

            stack.add( new LogicalGraphFilter( cluster, cluster.traitSet(), node, condition ) );
        }


        private RexNode getCondition() {
            List<RexNode> nodes = new ArrayList<>();
            while ( !rexStack.isEmpty() ) {
                nodes.add( 0, rexStack.pop() );
            }

            return nodes.size() == 1 ? nodes.get( 0 ) : algBuilder.and( nodes );
        }


        public void add( AlgNode node ) {
            this.stack.add( node );
        }


        public void add( RexNode node ) {
            this.rexStack.add( node );
        }


        public AlgNode build() {
            assert stack.size() == 1;
            return stack.pop();
        }


        public AlgNode peek() {
            return stack.peek();
        }


        public AlgNode pop() {
            return stack.pop();
        }


        public void addScanIfNecessary( List<Pair<String, RexNode>> nameAndProject ) {
            if ( !stack.isEmpty() ) {
                return;
            }
            if ( nameAndProject.size() == 1 ) {
                assert nameAndProject.get( 0 ).right.getType().getPolyType() == PolyType.NODE;
                stack.add( new LogicalGraphScan( cluster, catalogReader, cluster.traitSet(), graph, new AlgRecordType(
                        List.of(
                                new AlgDataTypeFieldImpl( nameAndProject.get( 0 ).left, 0, nodeType ) ) ) ) );
                return;
            } else if ( nameAndProject.size() == 2 ) {
                assert nameAndProject.get( 0 ).right.getType().getPolyType() == PolyType.NODE;
                assert nameAndProject.get( 1 ).right.getType().getPolyType() == PolyType.EDGE;

                addDefaultScanIfNecessary();
                return;
            }
            throw new UnsupportedOperationException();
        }

    }

}
