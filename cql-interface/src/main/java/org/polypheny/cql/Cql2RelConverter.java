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

package org.polypheny.cql.cql2rel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.polypheny.cql.exception.InvalidMethodInvocation;
import org.polypheny.cql.exception.UnexpectedTypeException;
import org.polypheny.cql.parser.BooleanGroup;
import org.polypheny.cql.parser.BooleanGroup.ColumnOpsBooleanOperators;
import org.polypheny.cql.parser.QueryNode;
import org.polypheny.cql.parser.SearchClause;
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

    public static RelRoot convert2Rel( final CqlResource cqlResource, RelBuilder relBuilder, RexBuilder rexBuilder ) {
        relBuilder = generateTableScan( cqlResource.queryRelation, relBuilder, rexBuilder );
        relBuilder = generateProjection( cqlResource.queryRelation, relBuilder, rexBuilder );
        relBuilder = generateFilters( cqlResource.filters, cqlResource.indexMapping, relBuilder, rexBuilder );
        RelNode relNode = relBuilder.build();

        final RelDataType rowType = relNode.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( relNode, relNode.getRowType(), SqlKind.SELECT, fields, collation );

        return root;
    }


    private static RelBuilder generateTableScan( Tree<Combiner, Index> tableOperations,
            RelBuilder relBuilder, RexBuilder rexBuilder ) {

        AtomicReference<RelBuilder> relBuilderAtomicReference = new AtomicReference<>( relBuilder );

        tableOperations.traverse( TraversalType.POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                try {
                    if ( treeNode.isLeaf() ) {
                        CatalogTable catalogTable = treeNode.getExternalNode().getCatalogTable();
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
                } catch ( InvalidMethodInvocation e ) {
                    throw new RuntimeException( "This exception will never be thrown since queryRelation only has "
                            + "table type indices." );
                }
            }
            return true;
        } );

        return relBuilderAtomicReference.get();
    }


    private static RelBuilder generateProjection( Tree<Combiner, Index> queryRelation, RelBuilder relBuilder,
            RexBuilder rexBuilder ) {

        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();
        Catalog catalog = Catalog.getInstance();
        AtomicInteger ordinal = new AtomicInteger();

        queryRelation.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE && treeNode.isLeaf() ) {
                try {
                    Index index = treeNode.getExternalNode();
                    String columnNamePrefix = index.fullyQualifiedName + ".";
                    CatalogTable catalogTable = index.getCatalogTable();
                    for ( Long columnId : catalogTable.columnIds ) {
                        RexNode inputRef = rexBuilder.makeInputRef( baseNode, ordinal.get() );
                        inputRefs.add( inputRef );
                        CatalogColumn column = catalog.getColumn( columnId );
                        columnNames.add( columnNamePrefix + column.name );
                        ordinal.getAndIncrement();
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode method." );
                } catch ( InvalidMethodInvocation e ) {
                    throw new RuntimeException( "This exception will never be thrown since queryRelation only has "
                            + "table type indices." );
                }
            }

            return true;
        } );

        relBuilder = relBuilder.project( inputRefs, columnNames, true );
        return relBuilder;
    }


    private static RelBuilder generateFilters( QueryNode filters, Map<String, Index> indexMapping,
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
                        SearchClause searchClause = treeNode.getExternalNode();
                        Filter filter = Filter.createFilter( searchClause, indexMapping );
                        RelDataTypeField relDataTypeField = filterMap.get( searchClause.indexStr );
                        rexNode = filter.convert2RexNode( baseNode, rexBuilder, relDataTypeField );
                    } else {
                        BooleanGroup booleanGroup = treeNode.getInternalNode();
                        if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperators.AND ) {
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.AND, secondToLastRexNode.get(), lastRexNode.get() );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperators.OR ) {
                            rexNode = rexBuilder.makeCall( SqlStdOperatorTable.OR, secondToLastRexNode.get(), lastRexNode.get() );
                        } else if ( booleanGroup.booleanOperator == ColumnOpsBooleanOperators.NOT ) {
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

}
