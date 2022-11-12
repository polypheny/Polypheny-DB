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

package org.polypheny.db.cql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.cql.BooleanGroup.ColumnOpsBooleanOperator;
import org.polypheny.db.cql.exception.UnexpectedTypeException;
import org.polypheny.db.cql.utils.Tree;
import org.polypheny.db.cql.utils.Tree.NodeType;
import org.polypheny.db.cql.utils.Tree.TraversalType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;


/**
 * Packaging information and algorithm to convert a {@link CqlQuery}
 * to relational algebra ({@link AlgNode}, {@link AlgRoot}, {@link RexNode})
 */
@Slf4j
public class Cql2RelConverter {

    private final CqlQuery cqlQuery;
    private final Map<Long, Integer> tableScanColumnOrdinalities;
    private final Map<Long, Integer> projectionColumnOrdinalities;


    public Cql2RelConverter( final CqlQuery cqlQuery ) {
        this.cqlQuery = cqlQuery;
        this.tableScanColumnOrdinalities = new HashMap<>();
        this.projectionColumnOrdinalities = new HashMap<>();
    }


    /**
     * Packaging of all algorithms involved in converting {@link CqlQuery}
     * to relational algebra.
     *
     * @param algBuilder {@link AlgBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link AlgRoot}.
     */
    public AlgRoot convert2Rel( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        algBuilder = generateScan( algBuilder, rexBuilder );
        if ( cqlQuery.filters != null ) {
            algBuilder = generateProjections( algBuilder, rexBuilder );
            algBuilder = generateFilters( algBuilder, rexBuilder );
            if ( cqlQuery.projections.exists() ) {
                algBuilder = cqlQuery.projections.convert2Rel( tableScanColumnOrdinalities, algBuilder, rexBuilder );
                projectionColumnOrdinalities.putAll( cqlQuery.projections.getProjectionColumnOrdinalities() );
            }
        } else {
            if ( cqlQuery.projections.exists() ) {
                setScanColumnOrdinalities();
                if ( cqlQuery.projections.hasAggregations() ) {
                    algBuilder = cqlQuery.projections
                            .convert2Rel( tableScanColumnOrdinalities, algBuilder, rexBuilder );
                } else {
                    algBuilder = cqlQuery.projections
                            .convert2RelForSingleProjection( tableScanColumnOrdinalities, algBuilder, rexBuilder );
                }
                projectionColumnOrdinalities.putAll( cqlQuery.projections.getProjectionColumnOrdinalities() );
            } else {
                algBuilder = generateProjections( algBuilder, rexBuilder );
            }
        }
        if ( cqlQuery.sortSpecifications != null && cqlQuery.sortSpecifications.size() != 0 ) {
            algBuilder = generateSort( algBuilder, rexBuilder );
        }
        AlgNode algNode = algBuilder.build();

        final AlgDataType rowType = algNode.getRowType();
        final List<Pair<Integer, String>> fields =
                Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;

        return new AlgRoot( algNode, algNode.getRowType(), Kind.SELECT, fields, collation );
    }


    private void setScanColumnOrdinalities() {
        cqlQuery.queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                TableIndex tableIndex = treeNode.getExternalNode();
                for ( Long columnId : tableIndex.catalogTable.fieldIds ) {
                    tableScanColumnOrdinalities.put( columnId, tableScanColumnOrdinalities.size() );
                }
            }
            return true;
        } );
    }


    /**
     * Generates table scan i.e. Combines all the tables that
     * are to be queried using the {@link Combiner}.
     *
     * @param algBuilder {@link AlgBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link AlgBuilder}.
     */
    private AlgBuilder generateScan( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating table scan." );
        Tree<Combiner, TableIndex> tableOperations = cqlQuery.queryRelation;
        AtomicReference<AlgBuilder> algBuilderAtomicReference = new AtomicReference<>( algBuilder );

        tableOperations.traverse( TraversalType.POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                try {
                    if ( treeNode.isLeaf() ) {
                        CatalogTable catalogTable = treeNode.getExternalNode().catalogTable;
                        algBuilderAtomicReference.set(
                                algBuilderAtomicReference.get().scan( catalogTable.getNamespaceName(), catalogTable.name )
                        );
                    } else {
                        Combiner combiner = treeNode.getInternalNode();
                        algBuilderAtomicReference.set(
                                combiner.combine( algBuilderAtomicReference.get(), rexBuilder )
                        );
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been "
                            + "made before calling the getExternalNode and getInternalNode methods.", e );
                }
            }
            return true;
        } );

        return algBuilderAtomicReference.get();
    }


    /**
     * Generate initial projection and set the ordinalities for all columns.
     * This projection, simply, maps the order in which tables were scanned
     * to the order in which columns are placed.
     *
     * These projections and ordinalities will be later used for getting column
     * references for filtering ({@link #generateFilters(AlgBuilder, RexBuilder)}),
     * sorting ({@link #generateSort(AlgBuilder, RexBuilder)}), aggregating
     * ({@link Projections#convert2Rel(Map, AlgBuilder, RexBuilder)}), etc.
     *
     * @param algBuilder {@link AlgBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link AlgBuilder}.
     */
    private AlgBuilder generateProjections( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating initial projection." );
        Tree<Combiner, TableIndex> queryRelation = cqlQuery.queryRelation;
        AlgNode baseNode = algBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        Catalog catalog = Catalog.getInstance();

        queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                try {
                    TableIndex tableIndex = treeNode.getExternalNode();
                    String columnNamePrefix = tableIndex.fullyQualifiedName + ".";
                    CatalogTable catalogTable = tableIndex.catalogTable;
                    for ( Long columnId : catalogTable.fieldIds ) {
                        int ordinal = tableScanColumnOrdinalities.size();
                        RexNode inputRef = rexBuilder.makeInputRef( baseNode, ordinal );
                        inputRefs.add( inputRef );
                        CatalogColumn column = catalog.getColumn( columnId );
                        columnNames.add( columnNamePrefix + column.name );
                        tableScanColumnOrdinalities.put( columnId, ordinal );
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been"
                            + " made before calling the getExternalNode method.", e );
                }
            }

            return true;
        } );

        algBuilder = algBuilder.project( inputRefs, columnNames, true );
        return algBuilder;
    }


    /**
     * Convert {@link Filter}s to relational algebra.
     *
     * @param algBuilder {@link AlgBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link AlgBuilder}.
     */
    private AlgBuilder generateFilters( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating filters." );
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> filters = cqlQuery.filters;
        if ( filters == null ) {
            return algBuilder;
        }
        AlgNode baseNode = algBuilder.peek();
        AtomicReference<RexNode> lastRexNode = new AtomicReference<>();
        AtomicReference<RexNode> secondToLastRexNode = new AtomicReference<>();

        AlgDataType filtersRowType = baseNode.getRowType();
        List<AlgDataTypeField> filtersRows = filtersRowType.getFieldList();
        Map<String, AlgDataTypeField> filterMap = new HashMap<>();
        filtersRows.forEach( ( r ) -> filterMap.put( r.getKey(), r ) );

        filters.traverse( TraversalType.POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                try {
                    RexNode rexNode;
                    if ( treeNode.isLeaf() ) {
                        Filter filter = treeNode.getExternalNode();
                        rexNode = filter.convert2RexNode( baseNode, rexBuilder, filterMap );
                    } else {
                        BooleanGroup<ColumnOpsBooleanOperator> booleanGroup = treeNode.getInternalNode();
                        if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.AND ) {
                            log.debug( "Found 'AND'." );
                            rexNode = rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.AND ),
                                    secondToLastRexNode.get(),
                                    lastRexNode.get()
                            );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.OR ) {
                            log.debug( "Found 'OR'." );
                            rexNode = rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.OR ),
                                    secondToLastRexNode.get(),
                                    lastRexNode.get()
                            );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.NOT ) {
                            log.debug( "Found 'NOT'." );
                            rexNode = rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.NOT ),
                                    lastRexNode.get()
                            );
                            rexNode = rexBuilder.makeCall(
                                    OperatorRegistry.get( OperatorName.AND ),
                                    secondToLastRexNode.get(),
                                    rexNode
                            );
                        } else {
//                            TODO: Implement PROX.
                            log.error( "Found 'PROX'. 'PROX' boolean operator not implemented." );
                            throw new RuntimeException( "'PROX' boolean operator not implemented." );
                        }
                    }
                    secondToLastRexNode.set( lastRexNode.get() );
                    lastRexNode.set( rexNode );
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been"
                            + " made before calling the getExternalNode method.", e );
                }
            }

            return true;
        } );

        algBuilder = algBuilder.filter( lastRexNode.get() );

        return algBuilder;
    }


    /**
     * Convert sort specifications to relational algebra.
     *
     * @param algBuilder {@link AlgBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link AlgBuilder}.
     */
    private AlgBuilder generateSort( AlgBuilder algBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating sort." );
        List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications = cqlQuery.sortSpecifications;
        List<RexNode> sortingNodes = new ArrayList<>();
        AlgNode baseNode = algBuilder.peek();
        for ( Pair<ColumnIndex, Map<String, Modifier>> sortSpecification : sortSpecifications ) {
            ColumnIndex columnIndex = sortSpecification.left;
            int ordinality = projectionColumnOrdinalities.get( columnIndex.catalogColumn.id );
            RexNode sortingNode = rexBuilder.makeInputRef( baseNode, ordinality );

            // TODO: Handle Modifiers

            sortingNodes.add( sortingNode );
        }

        algBuilder = algBuilder.sort( sortingNodes );
        return algBuilder;
    }

}
