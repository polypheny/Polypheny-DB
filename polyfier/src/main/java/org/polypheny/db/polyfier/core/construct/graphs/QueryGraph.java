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

package org.polypheny.db.polyfier.core.construct.graphs;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.polypheny.db.polyfier.core.construct.nodes.*;
import org.polypheny.db.polyfier.core.construct.model.Column;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class QueryGraph {
    private final Graph<Node, DefaultEdge> constructionGraph;

    private Node last;

    public QueryGraph() {
        this.constructionGraph = new SimpleDirectedGraph<>( DefaultEdge.class );
    }

    public void addScan( @NonNull Scan node ) {
        constructionGraph.addVertex( node );
    }

    public void addUnary( @NonNull Unary parent, @NonNull Node child ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Chosen Operation: " + parent.getOperatorType() + " on (" + child.getOperatorType() + ")"  );
        }
        constructionGraph.addVertex( parent );
        constructionGraph.addEdge( parent, child );
        last = parent;
    }

    public void addBinary( @NonNull Binary parent, @NonNull Pair<Node, Node> children ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Chosen Operation: " + parent.getOperatorType() + " on (" + children.left.getOperatorType() + " : " + children.left.getOperatorType() + ")"  );
        }
        constructionGraph.addVertex( parent );
        constructionGraph.addEdge( parent, children.left );
        constructionGraph.addEdge( parent, children.right );
        last = parent;
    }

    private List<Node> depthFirstOrder() {
        LinkedList<Node> xs = new LinkedList<>();
        depthFirstOrder( xs, last, 1 );
        return xs;
    }

    private void depthFirstOrder( LinkedList<Node> xs, Node node, int depth ) {
        if ( node == null ) {
            return;
        }
        if ( node instanceof Unary ) {
            depthFirstOrder(xs, ((Unary) node).getTarget(), depth + 1);
        }
        if ( node instanceof Binary ) {
            depthFirstOrder( xs, ((Binary) node).getTarget().left, depth + 1 );
            depthFirstOrder( xs, ((Binary) node).getTarget().right, depth + 1 );
        }
        xs.add( node );
    }

    public AlgNode finish( Statement statement ) throws IllegalArgumentException {
        AlgBuilder algBuilder = AlgBuilder.create( statement );

        Iterator<Node> iterator = depthFirstOrder().stream().iterator();
        while ( iterator.hasNext() ) {
            Node node = iterator.next();
            algBuilder = convert( algBuilder, node );
        }

        return algBuilder.build();
    }

    private AlgBuilder convert( final AlgBuilder algBuilder, Node node ) throws IllegalArgumentException {
        switch ( node.getOperatorType() ) {
            case UNION:
            case MINUS:
            case INTERSECT:
                SetNode setNode = (SetNode) node;
                try {
                    switch ( setNode.getOperatorType() ) {
                        case UNION:
                            algBuilder.union( false, 2 );
                            break;
                        case MINUS:
                            algBuilder.minus( false, 2 );
                            break;
                        case INTERSECT:
                            algBuilder.intersect( false, 2 );
                            break;
                    }
                } catch ( NullPointerException nullPointerException ) {
                    log.warn("Exception in conversion: ", nullPointerException);
                    throw new IllegalArgumentException("Conversion Error: ", nullPointerException);
                }
                algBuilder
                    .as( setNode.getAlias() )
                    .as( setNode.getTarget().left.getResult().getName() );
                return algBuilder;
            case JOIN:
                Join join = (Join) node;
                return algBuilder.join(
                        join.getJoinType(),
                        algBuilder.call( OperatorRegistry.get( join.getJoinOperator() ),
                        algBuilder.field(
                                2,
                                join.getJoinTarget().left.nSplit( 0 ),
                                join.getJoinTarget().left.nSplit( 1 )
                        ),
                        algBuilder.field(
                                2,
                                join.getJoinTarget().right.nSplit( 0 ),
                                join.getJoinTarget().right.nSplit( 1 )
                        ) )
                );
            case SORT:
                Sort sort = (Sort) node;
                ArrayList<RexNode> columns = new ArrayList<>();
                for ( Pair<String, Sort.SortColumn> pair : sort.getSortTargets() ) {
                    String[] sortField = pair.right.getColumn().split("\\.");
                    String alias;
                    String column;
                    if ( pair.left == null ) {
                        alias = sortField[0];
                    } else {
                        alias = pair.left;
                    }
                    column = sortField[1];
                    if ( pair.right.getDirection() == -1  ) {
                        columns.add( algBuilder.desc( algBuilder.field( 1, alias, column ) ) );
                    } else {
                        columns.add( algBuilder.field( 1, alias, column ) );
                    }
                }
                return algBuilder.sort( columns );
            case AGGREGATE:
                Aggregate aggregate = (Aggregate) node;
                throw new RuntimeException();
            case FILTER:
                Filter filter = (Filter) node;
                String column = filter.getFilterField().nSplit( 1 );

                String alias;
                if ( filter.getFilterField().getAliases().isEmpty() ) {
                    alias = filter.getFilterField().nSplit( 0 );
                } else {
                    alias = filter.getFilterField().recentAlias();
                }

                if ( NumberUtils.isCreatable( String.valueOf( filter.getFilter() ) ) ) {
                    String fltVal = String.valueOf( filter.getFilter() );
                    Number flt;
                    double dbl = Double.parseDouble( fltVal );
                    flt = dbl;
                    if ( dbl % 1 == 0 ) {
                        if ( fltVal.contains(".") ) {
                            fltVal = fltVal.split("\\.")[0];
                        }
                        flt = Integer.parseInt( fltVal );
                    }
                    return algBuilder.filter(
                            algBuilder.call(
                                    OperatorRegistry.get( filter.getFilterOperator() ),
                                    algBuilder.field( 1, alias, column ),
                                    algBuilder.literal( flt ) )
                    );
                } else {
                    return algBuilder.filter( algBuilder.call(
                            OperatorRegistry.get( filter.getFilterOperator() ),
                            algBuilder.field( 1, alias, column ),
                            algBuilder.literal( filter.getFilter() ) )
                    );
                }
            case PROJECT:
                Project project = (Project) node;

                ArrayList<RexNode> fields = new ArrayList<>();

                for ( Column col : project.getProjectFields() ) {
                    try {
                        fields.add( algBuilder.field( 1, col.nSplit( 0 ), col.nSplit( 1 ) ) );
                    } catch ( IllegalArgumentException illegalArgumentException ) {
                        log.warn("Exception in conversion: ", illegalArgumentException);
                        throw illegalArgumentException;
                    }
                }
                return algBuilder.project( fields );
            case SCAN:
                Scan scan = (Scan) node;
                return algBuilder.scan( Util.tokenize( scan.getTableName(), "." ) ).as( scan.getTableName() );
        }

        return algBuilder;
    }


}
