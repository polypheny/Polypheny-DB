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

package org.polypheny.cql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.polypheny.cql.BooleanGroup.ColumnOpsBooleanOperator;
import org.polypheny.cql.exception.UnexpectedTypeException;
import org.polypheny.cql.utils.Tree;
import org.polypheny.cql.utils.Tree.NodeType;
import org.polypheny.cql.utils.Tree.TraversalType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;

public class Cql2RelConverter {

    private final Map<String, Integer> columnOrdinalities = new HashMap<>();

    public RelRoot convert2Rel( final CqlQuery cqlQuery, RelBuilder relBuilder, RexBuilder rexBuilder ) {
        relBuilder = generateTableScan( cqlQuery.queryRelation, relBuilder, rexBuilder );
        relBuilder = generateProjections( cqlQuery.queryRelation, relBuilder, rexBuilder );
        if ( cqlQuery.filters != null ) {
            relBuilder = generateFilters( cqlQuery.filters, relBuilder, rexBuilder );
        }
        relBuilder = generateProjections( cqlQuery.projections, relBuilder, rexBuilder );
        if ( cqlQuery.sortSpecifications != null && cqlQuery.sortSpecifications.size() != 0 ) {
            relBuilder = generateSort( cqlQuery.sortSpecifications, relBuilder, rexBuilder );
        }
        RelNode relNode = relBuilder.build();

        final RelDataType rowType = relNode.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;

        return new RelRoot( relNode, relNode.getRowType(), SqlKind.SELECT, fields, collation );
    }


    private RelBuilder generateTableScan( Tree<Combiner, TableIndex> tableOperations,
            RelBuilder relBuilder, RexBuilder rexBuilder ) {

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
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode and getInternalNode methods." );
                }
            }
            return true;
        } );

        return relBuilderAtomicReference.get();
    }


    private RelBuilder generateProjections( List<Pair<ColumnIndex, Map<String, Modifier>>> projections,
            RelBuilder relBuilder, RexBuilder rexBuilder ) {

        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();

        for ( Pair<ColumnIndex, Map<String, Modifier>> projection : projections ) {
            ColumnIndex columnIndex = projection.left;
            int ordinality = columnOrdinalities.get( columnIndex.fullyQualifiedName );
            RexNode inputRef = rexBuilder.makeInputRef( baseNode, ordinality );
            inputRefs.add( inputRef );
        }

        relBuilder = relBuilder.project( inputRefs );

        return relBuilder;
    }


    private RelBuilder generateProjections( Tree<Combiner, TableIndex> queryRelation, RelBuilder relBuilder,
            RexBuilder rexBuilder ) {

        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        Catalog catalog = Catalog.getInstance();
        AtomicInteger ordinal = new AtomicInteger();

        queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                try {
                    TableIndex tableIndex = treeNode.getExternalNode();
                    String columnNamePrefix = tableIndex.fullyQualifiedName + ".";
                    CatalogTable catalogTable = tableIndex.catalogTable;
                    for ( Long columnId : catalogTable.columnIds ) {
                        RexNode inputRef = rexBuilder.makeInputRef( baseNode, ordinal.get() );
                        inputRefs.add( inputRef );
                        CatalogColumn column = catalog.getColumn( columnId );
                        columnNames.add( columnNamePrefix + column.name );
                        columnOrdinalities.put( columnNamePrefix + column.name, ordinal.get() );
                        ordinal.getAndIncrement();
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode method." );
                }
            }

            return true;
        } );

        relBuilder = relBuilder.project( inputRefs, columnNames, true );
        return relBuilder;
    }


    private RelBuilder generateFilters( Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> filters,
            RelBuilder relBuilder, RexBuilder rexBuilder ) {

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
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.AND, secondToLastRexNode.get(), lastRexNode.get() );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.OR ) {
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.OR, secondToLastRexNode.get(), lastRexNode.get() );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperator.NOT ) {
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.NOT, lastRexNode.get() );
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.AND, secondToLastRexNode.get(), rexNode );
                        } else {
                            throw new RuntimeException( "Not Implemented!" );
                        }
                    }
                    secondToLastRexNode.set( lastRexNode.get() );
                    lastRexNode.set( rexNode );
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode method." );
                }
            }

            return true;
        } );

        relBuilder = relBuilder.filter( lastRexNode.get() );

        return relBuilder;
    }


    private RelBuilder generateSort( List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications,
            RelBuilder relBuilder, RexBuilder rexBuilder ) {

        List<RexNode> sortingNodes = new ArrayList<>();
        RelNode baseNode = relBuilder.peek();
        for ( Pair<ColumnIndex, Map<String, Modifier>> sortSpecification : sortSpecifications ) {
            ColumnIndex columnIndex = sortSpecification.left;
            int ordinality = columnOrdinalities.get( columnIndex.fullyQualifiedName );
            RexNode sortingNode = rexBuilder.makeInputRef( baseNode, ordinality );

            // TODO: Handle Modifiers

            sortingNodes.add( sortingNode );
        }

        relBuilder = relBuilder.sort( sortingNodes );
        return relBuilder;
    }
}
