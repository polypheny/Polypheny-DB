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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.math.NumberUtils;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.*;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Alias;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Column;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.http.model.SortDirection;
import org.polypheny.db.http.model.SortState;
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
public class ConstructionGraph {
    private final Graph<Node, DefaultEdge> constructionGraph;

    private Node last;

    public ConstructionGraph() {
        this.constructionGraph = new SimpleDirectedGraph<>( DefaultEdge.class );
    }

    public void addScan( @NonNull Scan node ) {
        constructionGraph.addVertex( node );
    }

    public void addUnary( @NonNull Unary parent, @NonNull Node child ) {
        constructionGraph.addVertex( parent );
        constructionGraph.addEdge( parent, child );
        last = parent;
    }

    public void addBinary( @NonNull Binary parent, @NonNull Pair<Node, Node> children ) {
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
        log.debug( node.getOperatorType().toString() + " " + node.getResult().getColumns().stream().map( Column::getPolyType ) );
        xs.add( node );
    }

    public AlgNode finish( Statement statement ) {
        // final GraphIterator<Node, DefaultEdge> iterator = new DepthFirstIterator<>( constructionGraph );
        AlgBuilder algBuilder = AlgBuilder.create( statement );

        List<Node> xs = depthFirstOrder();
        Iterator<Node> iterator = xs.stream().iterator();

        log.debug("CONVERTING TREE: " + xs.stream().map( Node::getOperatorType ).collect(Collectors.toList()).toString() );

        while ( iterator.hasNext() ) {
            Node node = iterator.next();
            algBuilder = convert( algBuilder, node );
        }

        return algBuilder.build();
    }

    private AlgBuilder convert( final AlgBuilder algBuilder, Node node ) {
        switch ( node.getOperatorType() ) {
            case UNION:
            case MINUS:
            case INTERSECT:
                SetNode setNode = (SetNode) node;

                switch ( setNode.getOperatorType() ) {
                    case UNION:
                        algBuilder.union( true, 2 );
                        break;
                    case MINUS:
                        algBuilder.minus( true, 2 );
                        break;
                    case INTERSECT:
                        algBuilder.intersect( true, 2 );
                        break;
                }

                setNode.getResult().getAlias().ifPresent(
                        alias -> algBuilder
                                .as( alias.getAlias() )
                                .as( setNode.getTarget().left.getResult().getName() )
                );
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
                for ( Pair<Alias, SortState> pair : sort.getSortTargets() ) {
                    String[] sortField = pair.right.column.split("\\.");
                    String alias;
                    String column;
                    if ( pair.left == null ) {
                        alias = sortField[0];
                    } else {
                        alias = pair.left.getAlias();
                    }
                    column = sortField[1];
                    if ( pair.right.direction == SortDirection.DESC ) {
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
                if ( filter.getFilterField().getAlias().isPresent() ) {
                    alias = filter.getFilterField().nSplit( 0 );
                } else {
                    alias = filter.getFilterField().getAlias().get().getAlias();
                }

                if ( NumberUtils.isNumber( filter.getFilter() ) ) {
                    Number flt;
                    double dbl = Double.parseDouble( filter.getFilter() );
                    flt = dbl;
                    if ( dbl % 1 == 0 ) {
                        flt = Integer.parseInt( filter.getFilter() );
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
                    fields.add( algBuilder.field( 1, col.nSplit( 0 ), col.nSplit( 1 ) ) );
                }
                return algBuilder.project( fields );
            case SCAN:
                Scan scan = (Scan) node;
                return algBuilder.scan( Util.tokenize( scan.getTableName(), "." ) ).as( scan.getTableName() );
        }

        return algBuilder;
    }

}
