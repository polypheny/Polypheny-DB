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

import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Pseudograph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.ConstructUtil;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.Node;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.OperatorType;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.Scan;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Column;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Result;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
@Slf4j
public class RCN {
    private final Graph<Column, DefaultEdge> rcnResembling;
    private final Graph<Column, DefaultEdge> rcnForeignKey;

    private Set<Column> present = new HashSet<>();

    public RCN() {
        rcnResembling = new Pseudograph<>( DefaultEdge.class );
        rcnForeignKey = new DefaultDirectedGraph<>( DefaultEdge.class );
    }

    public void addScanResult( Scan scan ) {
        present.addAll( scan.getResult().getColumns() );
        addScanResultResembling( scan );
        addScanResultForeignKeys( scan );
    }

    public void addScanResultResembling( Scan scan ) {
        Set<Column> columns = rcnResembling.vertexSet();
        scan.getResult().getColumns().forEach( rcnResembling::addVertex );
        scan.getResult().getColumns().forEach( v -> {
            for ( Column u : columns ) {
                if ( u.getPolyType() == v.getPolyType() ) {
                    rcnResembling.addEdge( u, v );
                }
            }
        });
    }

    public void addScanResultForeignKeys( Scan scan ) {
        scan.getResult().getColumns().forEach( rcnForeignKey::addVertex );

        Catalog catalog = Catalog.getInstance();
        CatalogTable catalogTable = catalog.getTable(scan.getTableId() );
        // Get Catalog and Foreign Key References
        List<List<Long>> foreignKeys = catalog.getForeignKeys( catalogTable.id )
                .stream()
                .map(key -> key.columnIds)
                .collect(Collectors.toList());
        List<List<Long>> referencing = catalog.getForeignKeys( catalogTable.id )
                .stream()
                .map(key -> key.referencedKeyColumnIds)
                .collect(Collectors.toList());
        List<Pair<List<Long>,List<Long>>> pairs = ConstructUtil.zip( foreignKeys, referencing );

        // Get all Vertexes
        Set<Column> columns = rcnResembling.vertexSet();

        // Compare all columns
        for ( Column v : columns ) {
            for ( Column u : columns ) {
                for ( Pair<List<Long>, List<Long>> pair : pairs ) {
                    if ( pair.left.contains( v.getId() ) && pair.right.contains( u.getId() ) ) {
                        rcnForeignKey.addEdge( v, u );
                        break;
                    } else if ( pair.left.contains( u.getId() ) && pair.right.contains( v.getId() ) )  {
                        rcnForeignKey.addEdge( u, v );
                        break;
                    }
                }
            }
        }
    }

    public void updateResults( List<Node> results ) {
        if ( false ) {
            List<OperatorType> debug = results.stream().map( Node::getOperatorType ).collect(Collectors.toList());
            log.debug("UPDATE".repeat(15));;
            int i = 1;
            for ( OperatorType operatorType : debug ) {
                log.debug("-".repeat(80));;
                log.debug(i + ": " + operatorType.getName() );
                log.debug(results.get( i - 1 ).getResult().getColumns().stream().map(Column::getName).collect(Collectors.toList()).toString());
                i++;
            }
            log.debug("-".repeat(80));;
        }

        present = new HashSet<>(
                ConstructUtil.flatten(
                        results.stream()
                                .map(Node::getResult)
                                .map(Result::getColumns)
                                .collect(Collectors.toList())
                )
        );
    }

    public Optional<List<Pair<Node, Node>>> setOperationsWithoutProjections( List<Node> nodes ) {
        List<Pair<Node, Node>> results = ConstructUtil.cross( nodes )
                .stream()
                .filter( this::allowsSetOperationWithoutProjection )
                .collect(Collectors.toList());
        if ( results.isEmpty() ) {
            return Optional.empty();
        }
        return Optional.of( results );
    }

    /**
     * Checks if a Set Operation is possible without reordering the columns.
     * @param nodes
     * @return
     */
    public boolean allowsSetOperationWithoutProjection( Pair<Node, Node> nodes ) {
        if ( nodes.left == nodes.right ) {
            // No Set operations with the same childnode.
            return false;
        }
        if ( nodes.left.getResult().getColumns().size() != nodes.right.getResult().getColumns().size() ) {
            // No Non-Project Set operations with unequal row sizes.
            return false;
        }
        return ConstructUtil.zip( nodes.left.getResult().getColumns(), nodes.right.getResult().getColumns() )
                .stream()
                .allMatch( columns -> rcnResembling.containsEdge( columns.left, columns.right ) );
    }

    /**
     * Checks if any reordering of columns allows for a Set Operation. We return all orders for
     * which a Set Operation may occur.
     * @param nodes
     * @return
     */
    public Optional<List<List<Column>>> allowsSetOperationWithProjection( Pair<Node, Node> nodes ) {
        // Todo
        List<List<Column>> orders = new LinkedList<>();
        for ( Column x : nodes.left.getResult().getColumns() ) {
            List<Column> ys = getResembling( x )
                    .stream()
                    .filter( nodes.right.getResult().getColumns()::contains )
                    .collect(Collectors.toList());
            if ( ys.isEmpty() ) {
                return Optional.empty();
            }
            orders.add(ys);
        }
        return Optional.of( orders );
    }

    public Optional<List<Pair<Column, Column>>> joinOperationsWithoutProjections( List<Node> nodes ) {
        List<Pair<Column, Column>> results = ConstructUtil.flatten( ConstructUtil.cross( nodes )
                .stream()
                .map( this::allowsJoinOperation )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .collect(Collectors.toList())
        );
        if ( results.isEmpty() ) {
            return Optional.empty();
        }
        return Optional.of( results );
    }

    /**
     * Checks if a join operation is possible on the columns.
     * @param nodes
     * @return
     */
    public Optional<List<Pair<Column, Column>>> allowsJoinOperation( Pair<Node, Node> nodes ) {
        List<Pair<Column, Column>> targets = new LinkedList<>();
        for ( Column xi : nodes.left.getResult().getColumns() ) {
            List<Column> ys = getReferences( xi )
                    .stream()
                    .filter( nodes.right.getResult().getColumns()::contains )
                    .collect(Collectors.toList());
            ys.forEach( yj -> targets.add( Pair.of( yj, xi )) );
            ys = getReferenced( xi )
                    .stream()
                    .filter( nodes.right.getResult().getColumns()::contains )
                    .collect( Collectors.toList() );
            ys.forEach( yj -> targets.add( Pair.of( xi, yj )) );
        }
        if ( targets.isEmpty() ) {
            return Optional.empty();
        }
        return Optional.of( targets );
    }

    private Set<Column> getResembling( Column column ) {
        return rcnResembling
                .outgoingEdgesOf( column )
                .stream()
                .map(rcnResembling::getEdgeTarget)
                .filter( present::contains )
                .collect(Collectors.toSet());
    }

    private Set<Column> getReferences( Column column ) {
        return rcnForeignKey
                .outgoingEdgesOf( column )
                .stream()
                .map(rcnForeignKey::getEdgeTarget)
                .filter( present::contains )
                .collect(Collectors.toSet());
    }

    private Set<Column> getReferenced( Column column ) {
        return rcnForeignKey.incomingEdgesOf( column )
                .stream()
                .map(rcnForeignKey::getEdgeSource)
                .filter( present::contains )
                .collect(Collectors.toSet());
    }

}
