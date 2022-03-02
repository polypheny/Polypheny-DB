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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.graph.LogicalGraph;
import org.polypheny.db.algebra.logical.graph.LogicalGraphFilter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphPattern;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherNode.CypherFamily;
import org.polypheny.db.cypher.CypherNode.CypherKind;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyRelationship;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

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
        assert parameters.databaseId != null;
        LogicalGraph graph = new LogicalGraph( parameters.getDatabaseId() );

        if ( !CypherFamily.QUERY.contains( query.getCypherKind() ) ) {
            throw new RuntimeException( "Used a unsupported query." );
        }

        CypherContext context = new CypherContext( query, graph, parameters.databaseId, cluster, algBuilder, rexBuilder );

        convertQuery( query, context );

        return AlgRoot.of( context.node, context.kind );
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
            default:
                throw new UnsupportedOperationException();
        }

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
        context.combineFilter();

        if ( clause.getWhere() == null ) {
            return;
        }

        convertWhere( clause.getWhere(), context );

        context.add( new LogicalGraphModify() );


    }


    private void convertPattern( CypherPattern pattern, CypherContext context ) {
        if ( context.active.getCypherKind() == CypherKind.CREATE ) {
            // convert "values" pattern (LogicalGraphPattern AlgNode)
            convertValuesPattern( pattern, context );
        } else {
            // convert filter pattern ( RexNode PATTERN_MATCH )
            convertFilterPattern( pattern, context );
        }

    }


    private void convertFilterPattern( CypherPattern pattern, CypherContext context ) {
    }


    private void convertValuesPattern( CypherPattern pattern, CypherContext context ) {
        switch ( pattern.getCypherKind() ) {
            case PATH:
                CypherEveryPathPattern path = (CypherEveryPathPattern) pattern;
                List<PolyNode> nodes = path.getNodes().stream().map( CypherNodePattern::getPolyNode ).collect( Collectors.toList() );
                List<CypherRelPattern> relationships = path.getRelationships();
                List<PolyRelationship> rels = new ArrayList<>();
                assert nodes.size() == relationships.size() + 1;

                Iterator<CypherRelPattern> relIter = relationships.iterator();
                PolyNode node = nodes.get( 0 );
                int i = 0;
                while ( relIter.hasNext() ) {
                    i++;
                    PolyNode next = nodes.get( i );
                    rels.add( relationships.get( i - 1 ).getPolyRelationship( node.id, next.id ) );
                    node = next;
                }

                context.add( new LogicalGraphPattern( cluster, cluster.traitSet(), ImmutableList.copyOf( nodes ), ImmutableList.copyOf( rels ) ) );
            case PATTERN:
            case REL_PATTERN:
            case NODE_PATTERN:
            case SHORTEST_PATTERN:
                throw new CypherSyntaxException( "Used pattern is not supported for Graph patterns." );
            default:
                throw new UnsupportedOperationException();
        }

    }


    private void setKindIfNull( CypherContext context, Kind kind ) {
        if ( context.kind == null ) {
            context.kind = kind;
        }
    }


    private static class CypherContext {

        private final AlgOptCluster cluster;
        private final AlgBuilder algBuilder;
        private final RexBuilder rexBuilder;
        private Kind kind;
        private Stack<AlgNode> stack = new Stack<>();
        private Stack<RexNode> rexStack = new Stack<>();
        final private CypherNode original;
        final private LogicalGraph graph;
        private CypherNode active;


        private CypherContext( CypherNode original, LogicalGraph graph, Long databaseId, AlgOptCluster cluster, AlgBuilder algBuilder, RexBuilder rexBuilder ) {
            this.original = original;
            this.graph = graph;
            this.cluster = cluster;
            this.algBuilder = algBuilder;
            this.rexBuilder = rexBuilder;
            //stack.add( new LogicalGraphScan( cluster, cluster.traitSet(), databaseId ) );
        }


        public void combineFilter() {
            assert stack.size() == 1;
            AlgNode node = stack.pop();
            List<RexNode> nodes = new ArrayList<>();
            while ( !rexStack.isEmpty() ) {
                nodes.add( 0, rexStack.pop() );
            }

            RexNode condition = nodes.size() == 1 ? nodes.get( 0 ) : algBuilder.and( nodes );

            LogicalGraphFilter filter = new LogicalGraphFilter( cluster, cluster.traitSet(), node, condition );

            stack.add( filter );
        }


        public void add( AlgNode node ) {
            this.stack.add( node );
        }

    }

}
