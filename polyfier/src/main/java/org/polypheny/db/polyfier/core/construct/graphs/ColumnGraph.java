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

import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Pseudograph;
import org.polypheny.db.polyfier.core.construct.ConstructUtil;
import org.polypheny.db.polyfier.core.construct.ConstructionAlgorithm;
import org.polypheny.db.polyfier.core.construct.nodes.Node;
import org.polypheny.db.polyfier.core.construct.nodes.Scan;
import org.polypheny.db.polyfier.core.construct.model.Column;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
@Slf4j
public class ColumnGraph {
    private final Graph<Column, DefaultEdge> rcnResembling;
    private final Graph<Column, DefaultEdge> rcnForeignKey;

    private Set<Node> present = new HashSet<>();

    public ColumnGraph() {
        rcnResembling = new Pseudograph<>( DefaultEdge.class );
        rcnForeignKey = new DefaultDirectedGraph<>( DefaultEdge.class );
    }

    public void addScanResult( Scan scan ) {
        present.add( scan );
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
        present = Set.of( results.toArray(new Node[0]) );
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

    public Optional<List<Pair<List<Column>, List<Column>>>> allowsSetOperationWithProjection( List<Node> nodes ) {
        log.debug("");
        final List<Pair<List<Column>, List<Column>>> projectionColumns = new LinkedList<>();
        ConstructUtil.cross( nodes )
                .stream()
                .map( this::allowsSetOperationWithProjection )
                .filter( Optional::isPresent )
                .filter( o -> o.get().left.size() > 0 )
                .forEach( o -> projectionColumns.add( o.get() ) );
        if ( projectionColumns.isEmpty() ) {
            return Optional.empty();
        }

        return Optional.of( projectionColumns );
    }

    /**
     * Checks if any reordering of columns allows for a Set Operation. We return all orders for
     * which a Set Operation may occur.
     */
    public Optional<Pair<List<Column>, List<Column>>> allowsSetOperationWithProjection( Pair<Node, Node> nodes ) {
        if ( nodes.left == nodes.right ) {
            // No Set operations with the same childnode.
            return Optional.empty();
        }
        ConstructionAlgorithm.PolyTypeSubsetResolver<PolyType> polyTypePolyTypeSubsetResolver = new ConstructionAlgorithm.PolyTypeSubsetResolver<>(
                nodes.left.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList()),
                nodes.right.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList())
        );

        if ( polyTypePolyTypeSubsetResolver.solved().isPresent() ) {

            ConstructionAlgorithm.PolyTypeSubsetResolver.ProjectionCodes projectionCodes = polyTypePolyTypeSubsetResolver.solved().get();

            List<Column> leftColumns = new LinkedList<>();
            for ( Integer code : projectionCodes.getProjectionCodeLeft() ) {
                leftColumns.add( nodes.left.getResult().getColumns().get( code ) );
            }
            List<Column> rightColumns = new LinkedList<>();
            for ( Integer code : projectionCodes.getProjectionCodeRight() ) {
                rightColumns.add( nodes.right.getResult().getColumns().get( code ) );
            }

            return Optional.of( Pair.of( leftColumns, rightColumns ) );

        } else {
            return Optional.empty();
        }
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
                .filter( this::columnExists )
                .collect(Collectors.toSet());
    }

    private Set<Column> getReferences( Column column ) {
        return rcnForeignKey
                .outgoingEdgesOf( column )
                .stream()
                .map(rcnForeignKey::getEdgeTarget)
                .filter( this::columnExists )
                .collect(Collectors.toSet());
    }

    private Set<Column> getReferenced( Column column ) {
        return rcnForeignKey.incomingEdgesOf( column )
                .stream()
                .map(rcnForeignKey::getEdgeSource)
                .filter( this::columnExists )
                .collect(Collectors.toSet());
    }

    private boolean columnExists( Column column ) {
        return this.present.contains( column.getResult().getNode() );
    }

}
