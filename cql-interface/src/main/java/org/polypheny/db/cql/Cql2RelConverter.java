/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.operators.OperatorName;
import org.polypheny.db.cql.BooleanGroup.ColumnOpsBooleanOperator;
import org.polypheny.db.cql.exception.UnexpectedTypeException;
import org.polypheny.db.cql.utils.Tree;
import org.polypheny.db.cql.utils.Tree.NodeType;
import org.polypheny.db.cql.utils.Tree.TraversalType;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;


/**
 * Packaging information and algorithm to convert a {@link CqlQuery}
 * to relational algebra ({@link RelNode}, {@link RelRoot}, {@link RexNode})
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
     * @param relBuilder {@link RelBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link RelRoot}.
     */
    public RelRoot convert2Rel( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        relBuilder = generateTableScan( relBuilder, rexBuilder );
        if ( cqlQuery.filters != null ) {
            relBuilder = generateProjections( relBuilder, rexBuilder );
            relBuilder = generateFilters( relBuilder, rexBuilder );
            if ( cqlQuery.projections.exists() ) {
                relBuilder = cqlQuery.projections.convert2Rel( tableScanColumnOrdinalities, relBuilder, rexBuilder );
                projectionColumnOrdinalities.putAll( cqlQuery.projections.getProjectionColumnOrdinalities() );
            }
        } else {
            if ( cqlQuery.projections.exists() ) {
                setTableScanColumnOrdinalities();
                if ( cqlQuery.projections.hasAggregations() ) {
                    relBuilder = cqlQuery.projections
                            .convert2Rel( tableScanColumnOrdinalities, relBuilder, rexBuilder );
                } else {
                    relBuilder = cqlQuery.projections
                            .convert2RelForSingleProjection( tableScanColumnOrdinalities, relBuilder, rexBuilder );
                }
                projectionColumnOrdinalities.putAll( cqlQuery.projections.getProjectionColumnOrdinalities() );
            } else {
                relBuilder = generateProjections( relBuilder, rexBuilder );
            }
        }
        if ( cqlQuery.sortSpecifications != null && cqlQuery.sortSpecifications.size() != 0 ) {
            relBuilder = generateSort( relBuilder, rexBuilder );
        }
        RelNode relNode = relBuilder.build();

        final RelDataType rowType = relNode.getRowType();
        final List<Pair<Integer, String>> fields =
                Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;

        return new RelRoot( relNode, relNode.getRowType(), Kind.SELECT, fields, collation );
    }


    private void setTableScanColumnOrdinalities() {
        cqlQuery.queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                TableIndex tableIndex = treeNode.getExternalNode();
                for ( Long columnId : tableIndex.catalogTable.columnIds ) {
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
     * @param relBuilder {@link RelBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link RelBuilder}.
     */
    private RelBuilder generateTableScan( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating table scan." );
        Tree<Combiner, TableIndex> tableOperations = cqlQuery.queryRelation;
        AtomicReference<RelBuilder> relBuilderAtomicReference = new AtomicReference<>( relBuilder );

        tableOperations.traverse( TraversalType.POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                try {
                    if ( treeNode.isLeaf() ) {
                        CatalogTable catalogTable = treeNode.getExternalNode().catalogTable;
                        relBuilderAtomicReference.set(
                                relBuilderAtomicReference.get().scan( catalogTable.getSchemaName(), catalogTable.name )
                        );
                    } else {
                        Combiner combiner = treeNode.getInternalNode();
                        relBuilderAtomicReference.set(
                                combiner.combine( relBuilderAtomicReference.get(), rexBuilder )
                        );
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been "
                            + "made before calling the getExternalNode and getInternalNode methods.", e );
                }
            }
            return true;
        } );

        return relBuilderAtomicReference.get();
    }


    /**
     * Generate initial projection and set the ordinalities for all columns.
     * This projection, simply, maps the order in which tables were scanned
     * to the order in which columns are placed.
     *
     * These projections and ordinalities will be later used for getting column
     * references for filtering ({@link #generateFilters(RelBuilder, RexBuilder)}),
     * sorting ({@link #generateSort(RelBuilder, RexBuilder)}), aggregating
     * ({@link Projections#convert2Rel(Map, RelBuilder, RexBuilder)}), etc.
     *
     * @param relBuilder {@link RelBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link RelBuilder}.
     */
    private RelBuilder generateProjections( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating initial projection." );
        Tree<Combiner, TableIndex> queryRelation = cqlQuery.queryRelation;
        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        Catalog catalog = Catalog.getInstance();

        queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                try {
                    TableIndex tableIndex = treeNode.getExternalNode();
                    String columnNamePrefix = tableIndex.fullyQualifiedName + ".";
                    CatalogTable catalogTable = tableIndex.catalogTable;
                    for ( Long columnId : catalogTable.columnIds ) {
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

        relBuilder = relBuilder.project( inputRefs, columnNames, true );
        return relBuilder;
    }


    /**
     * Convert {@link Filter}s to relational algebra.
     *
     * @param relBuilder {@link RelBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link RelBuilder}.
     */
    private RelBuilder generateFilters( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating filters." );
        Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> filters = cqlQuery.filters;
        if ( filters == null ) {
            return relBuilder;
        }
        RelNode baseNode = relBuilder.peek();
        AtomicReference<RexNode> lastRexNode = new AtomicReference<>();
        AtomicReference<RexNode> secondToLastRexNode = new AtomicReference<>();

        RelDataType filtersRowType = baseNode.getRowType();
        List<RelDataTypeField> filtersRows = filtersRowType.getFieldList();
        Map<String, RelDataTypeField> filterMap = new HashMap<>();
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
                                    StdOperatorRegistry.get( OperatorName.AND ),
                                    secondToLastRexNode.get(),
                                    lastRexNode.get()
                            );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.OR ) {
                            log.debug( "Found 'OR'." );
                            rexNode = rexBuilder.makeCall(
                                    StdOperatorRegistry.get( OperatorName.OR ),
                                    secondToLastRexNode.get(),
                                    lastRexNode.get()
                            );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.NOT ) {
                            log.debug( "Found 'NOT'." );
                            rexNode = rexBuilder.makeCall(
                                    StdOperatorRegistry.get( OperatorName.NOT ),
                                    lastRexNode.get()
                            );
                            rexNode = rexBuilder.makeCall(
                                    StdOperatorRegistry.get( OperatorName.AND ),
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

        relBuilder = relBuilder.filter( lastRexNode.get() );

        return relBuilder;
    }


    /**
     * Convert sort specifications to relational algebra.
     *
     * @param relBuilder {@link RelBuilder}.
     * @param rexBuilder {@link RexBuilder}.
     * @return {@link RelBuilder}.
     */
    private RelBuilder generateSort( RelBuilder relBuilder, RexBuilder rexBuilder ) {
        log.debug( "Generating sort." );
        List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications = cqlQuery.sortSpecifications;
        List<RexNode> sortingNodes = new ArrayList<>();
        RelNode baseNode = relBuilder.peek();
        for ( Pair<ColumnIndex, Map<String, Modifier>> sortSpecification : sortSpecifications ) {
            ColumnIndex columnIndex = sortSpecification.left;
            int ordinality = projectionColumnOrdinalities.get( columnIndex.catalogColumn.id );
            RexNode sortingNode = rexBuilder.makeInputRef( baseNode, ordinality );

            // TODO: Handle Modifiers

            sortingNodes.add( sortingNode );
        }

        relBuilder = relBuilder.sort( sortingNodes );
        return relBuilder;
    }

}
