/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.restapi;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.javalin.http.Context;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.restapi.RequestParser.Filters;
import org.polypheny.db.restapi.exception.RestException;
import org.polypheny.db.restapi.models.requests.ResourceDeleteRequest;
import org.polypheny.db.restapi.models.requests.ResourceGetRequest;
import org.polypheny.db.restapi.models.requests.ResourcePatchRequest;
import org.polypheny.db.restapi.models.requests.ResourcePostRequest;
import org.polypheny.db.restapi.models.requests.ResourceValuesRequest;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilder.GroupKey;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;


@Slf4j
public class Rest {

    private final TransactionManager transactionManager;
    private final long databaseId;
    private final long userId;


    Rest( final TransactionManager transactionManager, final long userId, final long databaseId ) {
        this.transactionManager = transactionManager;
        this.databaseId = databaseId;
        this.userId = userId;
    }


    String processGetResource( final ResourceGetRequest resourceGetRequest, final Context ctx ) throws RestException {
        if ( log.isDebugEnabled() ) {
            log.debug( "Starting to process get resource request. Session ID: {}.", ctx.req.getSession().getId() );
        }
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        // Table Scans
        algBuilder = this.tableScans( algBuilder, rexBuilder, resourceGetRequest.tables );

        // Initial projection
        algBuilder = this.initialProjection( algBuilder, rexBuilder, resourceGetRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, algBuilder, rexBuilder, resourceGetRequest.filters, ctx.req );
        if ( filters != null ) {
            algBuilder = algBuilder.filter( filters );
        }

        // Aggregates
        algBuilder = this.aggregates( algBuilder, rexBuilder, resourceGetRequest.requestColumns, resourceGetRequest.groupings );

        // Final projection
        algBuilder = this.finalProjection( algBuilder, rexBuilder, resourceGetRequest.requestColumns );

        // Sorts, limit, offset
        algBuilder = this.sort( algBuilder, rexBuilder, resourceGetRequest.sorting, resourceGetRequest.limit, resourceGetRequest.offset );

        if ( log.isDebugEnabled() ) {
            log.debug( "AlgNodeBuilder: {}", algBuilder.toString() );
        }
        AlgNode algNode = algBuilder.build();
        log.debug( "AlgNode was built." );

        // Wrap {@link AlgNode} into a AlgRoot
        final AlgDataType rowType = algNode.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( PolyTypeUtil.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( algNode, algNode.getTupleType(), Kind.SELECT, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    String processPatchResource( final ResourcePatchRequest resourcePatchRequest, final Context ctx, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        Snapshot snapshot = statement.getTransaction().getSnapshot();

        LogicalTable table = snapshot.rel().getTable( resourcePatchRequest.tables.get( 0 ).id ).orElseThrow();

        // Table Scans
        algBuilder = this.tableScans( algBuilder, rexBuilder, resourcePatchRequest.tables );

        // Initial projection
        algBuilder = this.initialProjection( algBuilder, rexBuilder, resourcePatchRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, algBuilder, rexBuilder, resourcePatchRequest.filters, ctx.req );
        if ( filters != null ) {
            algBuilder = algBuilder.filter( filters );
        }

        // Table Modify

        AlgPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgCluster cluster = AlgCluster.create( planner, rexBuilder, null, Catalog.getInstance().getSnapshot() );

        // Values
        AlgDataType tableRowType = table.getTupleType();
        List<AlgDataTypeField> tableRows = tableRowType.getFields();
        List<String> valueColumnNames = this.valuesColumnNames( resourcePatchRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, algBuilder, rexBuilder, resourcePatchRequest, tableRows, inputStreams ).get( 0 );

        AlgNode algNode = algBuilder.build();
        RelModify<?> modify = new LogicalRelModify(
                cluster,
                algNode.getTraitSet(),
                table,
                algNode,
                Modify.Operation.UPDATE,
                valueColumnNames,
                rexValues,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = modify.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( PolyTypeUtil.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( modify, rowType, Kind.UPDATE, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    String processDeleteResource( final ResourceDeleteRequest resourceDeleteRequest, final Context ctx ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        LogicalTable table = getLogicalTable( getTransaction().getSnapshot(), resourceDeleteRequest.tables.get( 0 ).getNamespaceName(), resourceDeleteRequest.tables.get( 0 ).getName() );

        // Table Scans
        algBuilder = this.tableScans( algBuilder, rexBuilder, resourceDeleteRequest.tables );

//         Initial projection
        algBuilder = this.initialProjection( algBuilder, rexBuilder, resourceDeleteRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, algBuilder, rexBuilder, resourceDeleteRequest.filters, ctx.req );
        if ( filters != null ) {
            algBuilder = algBuilder.filter( filters );
        }

        // Table Modify

        AlgPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgCluster cluster = AlgCluster.create( planner, rexBuilder, null, Catalog.getInstance().getSnapshot() );

        AlgNode algNode = algBuilder.build();
        RelModify<?> modify = new LogicalRelModify(
                cluster,
                algNode.getTraitSet(),
                table,
                algNode,
                Modify.Operation.DELETE,
                null,
                null,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = modify.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( PolyTypeUtil.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( modify, rowType, Kind.DELETE, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    private static LogicalTable getLogicalTable( Snapshot snapshot, String namespaceName, String tableName ) {
        return snapshot.rel().getTable( namespaceName, tableName ).orElseThrow();
    }


    String processPostResource( final ResourcePostRequest insertValueRequest, final Context ctx, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        LogicalTable table = getLogicalTable( transaction.getSnapshot(), insertValueRequest.tables.get( 0 ).getNamespaceName(), insertValueRequest.tables.get( 0 ).getName() );

        // Values
        AlgDataType tableRowType = table.getTupleType();
        List<AlgDataTypeField> tableRows = tableRowType.getFields();

        AlgPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgCluster cluster = AlgCluster.create( planner, rexBuilder, null, Catalog.getInstance().getSnapshot() );

        List<String> valueColumnNames = this.valuesColumnNames( insertValueRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, algBuilder, rexBuilder, insertValueRequest, tableRows, inputStreams ).get( 0 );
        algBuilder.push( LogicalRelValues.createOneRow( cluster ) );
        algBuilder.project( rexValues, valueColumnNames );

        // Table Modify
        AlgNode algNode = algBuilder.build();
        RelModify<?> modify = new LogicalRelModify(
                cluster,
                algNode.getTraitSet(),
                table,
                algNode,
                Modify.Operation.INSERT,
                null,
                null,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = modify.getTupleType();
        final List<Pair<Integer, String>> fields = Pair.zip( PolyTypeUtil.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( modify, rowType, Kind.INSERT, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    @VisibleForTesting
    AlgBuilder tableScans( AlgBuilder algBuilder, RexBuilder rexBuilder, List<LogicalTable> tables ) {
        boolean firstTable = true;
        for ( LogicalTable catalogTable : tables ) {
            if ( firstTable ) {
                algBuilder = algBuilder.relScan( catalogTable.getNamespaceName(), catalogTable.name );
                firstTable = false;
            } else {
                algBuilder = algBuilder
                        .relScan( catalogTable.getNamespaceName(), catalogTable.name )
                        .join( JoinAlgType.INNER, rexBuilder.makeLiteral( true ) );
            }
        }
        return algBuilder;
    }


    @VisibleForTesting
    List<RexNode> filters( Statement statement, AlgBuilder algBuilder, RexBuilder rexBuilder, Filters filters, HttpServletRequest req ) {
        if ( filters.literalFilters != null ) {
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "Starting to process filters. Session ID: {}.", req.getSession().getId() );
            }
            List<RexNode> filterNodes = new ArrayList<>();
            AlgNode baseNodeForFilters = algBuilder.peek();
            AlgDataType filtersRowType = baseNodeForFilters.getTupleType();
            List<AlgDataTypeField> filtersRows = filtersRowType.getFields();
            Map<String, AlgDataTypeField> filterMap = new HashMap<>();
            filtersRows.forEach( ( r ) -> filterMap.put( r.getName(), r ) );
            int index = 0;
            for ( RequestColumn column : filters.literalFilters.keySet() ) {
                for ( Pair<Operator, PolyValue> filterOperationPair : filters.literalFilters.get( column ) ) {
                    AlgDataTypeField typeField = filterMap.get( column.getColumn().name );
                    RexNode inputRef = rexBuilder.makeInputRef( baseNodeForFilters, typeField.getIndex() );
                    PolyValue param = filterOperationPair.right;
                    statement.getDataContext().addParameterValues( index, typeField.getType(), ImmutableList.of( param ) );
                    RexNode rightHandSide = rexBuilder.makeDynamicParam( typeField.getType(), index );
                    index++;
                    RexNode call = rexBuilder.makeCall( filterOperationPair.left, inputRef, rightHandSide );
                    filterNodes.add( call );
                }
            }

            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "Finished processing filters. Session ID: {}.", req.getSession().getId() );
            }
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "Added filters to relation. Session ID: {}.", req.getSession().getId() );
            }
            return filterNodes;
        } else {
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "No filters to add. Session ID: {}.", req.getSession().getId() );
            }
            return null;
        }
    }


    List<String> valuesColumnNames( List<List<Pair<RequestColumn, PolyValue>>> values ) {
        List<String> valueColumnNames = new ArrayList<>();
        List<Pair<RequestColumn, PolyValue>> rowsToInsert = values.get( 0 );
        for ( Pair<RequestColumn, PolyValue> insertValue : rowsToInsert ) {
            valueColumnNames.add( insertValue.left.getColumn().name );
        }

        return valueColumnNames;
    }


    List<List<RexNode>> valuesNode( Statement statement, AlgBuilder algBuilder, RexBuilder rexBuilder, ResourceValuesRequest request, List<AlgDataTypeField> tableRows, Map<String, InputStream> inputStreams ) {
        List<List<Pair<RequestColumn, PolyValue>>> values = request.values;
        List<List<RexNode>> wrapperList = new ArrayList<>();
        int index = 0;
        for ( List<Pair<RequestColumn, PolyValue>> rowsToInsert : values ) {
            List<RexNode> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, PolyValue> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                AlgDataTypeField typeField = tableRows.get( columnPosition );
                if ( inputStreams != null && request.useDynamicParams && typeField.getType().getPolyType().getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                    FileInputHandle fih = new FileInputHandle( statement, inputStreams.get( insertValue.left.getColumn().name ) );
                    statement.getDataContext().addParameterValues( index, typeField.getType(), ImmutableList.of( PolyBlob.of( fih.getData() ) ) );
                    rexValues.add( rexBuilder.makeDynamicParam( typeField.getType(), index ) );
                    index++;
                } else {
                    rexValues.add( rexBuilder.makeLiteral( insertValue.right, typeField.getType(), typeField.getType().getPolyType() ) );
                }
            }
            wrapperList.add( rexValues );
        }

        return wrapperList;
    }


    List<List<RexLiteral>> valuesLiteral( AlgBuilder algBuilder, RexBuilder rexBuilder, List<List<Pair<RequestColumn, Object>>> values, List<AlgDataTypeField> tableRows ) {
        List<List<RexLiteral>> wrapperList = new ArrayList<>();
        for ( List<Pair<RequestColumn, Object>> rowsToInsert : values ) {
            List<RexLiteral> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                AlgDataTypeField typeField = tableRows.get( columnPosition );
                rexValues.add( (RexLiteral) rexBuilder.makeLiteral( insertValue.right, typeField.getType(), true ) );
            }
            wrapperList.add( rexValues );
        }

        return wrapperList;
    }


    AlgBuilder initialProjection( AlgBuilder algBuilder, RexBuilder rexBuilder, List<RequestColumn> columns ) {
        AlgNode baseNode = algBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for ( RequestColumn column : columns ) {
            RexNode inputRef = rexBuilder.makeInputRef( baseNode, column.getScanIndex() );
            inputRefs.add( inputRef );
            aliases.add( column.getAlias() );
        }

        algBuilder = algBuilder.project( inputRefs );//,aliases, true );
        return algBuilder;
    }


    AlgBuilder finalProjection( AlgBuilder algBuilder, RexBuilder rexBuilder, List<RequestColumn> columns ) {
        AlgNode baseNode = algBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for ( RequestColumn column : columns ) {
            if ( column.isExplicit() ) {
                RexNode inputRef = rexBuilder.makeInputRef( baseNode, column.getLogicalIndex() );
                inputRefs.add( inputRef );
                aliases.add( column.getAlias() );
            }
        }

        algBuilder = algBuilder.project( inputRefs, aliases, true );
        return algBuilder;
    }


    AlgBuilder aggregates( AlgBuilder algBuilder, RexBuilder rexBuilder, List<RequestColumn> requestColumns, List<RequestColumn> groupings ) {
        AlgNode baseNodeForAggregation = algBuilder.peek();

        List<AggregateCall> aggregateCalls = new ArrayList<>();
        for ( RequestColumn column : requestColumns ) {
            if ( column.getAggregate() != null ) {
                List<Integer> inputFields = new ArrayList<>();
                inputFields.add( column.getLogicalIndex() );
                String fieldName = column.getAlias();
                AggregateCall aggregateCall = AggregateCall.create(
                        column.getAggregate(),
                        false,
                        false,
                        inputFields,
                        -1,
                        AlgCollations.EMPTY,
                        groupings.size(),
                        baseNodeForAggregation,
                        null,
                        fieldName );
                aggregateCalls.add( aggregateCall );
            }
        }

        if ( !aggregateCalls.isEmpty() ) {

            List<Integer> groupByOrdinals = new ArrayList<>();
            for ( RequestColumn column : groupings ) {
                groupByOrdinals.add( column.getLogicalIndex() );
            }

            GroupKey groupKey = algBuilder.groupKey( ImmutableBitSet.of( groupByOrdinals ) );

            algBuilder = algBuilder.aggregate( groupKey, aggregateCalls );
        }

        int groupingsLogicalIndex = 0;
        int aggregationsLogicalIndex = groupings.size();

        for ( RequestColumn requestColumn : requestColumns ) {
            if ( requestColumn.isAggregateColumn() ) {
                requestColumn.setLogicalIndex( aggregationsLogicalIndex );
                aggregationsLogicalIndex++;
            } else {
                requestColumn.setLogicalIndex( groupingsLogicalIndex );
                groupingsLogicalIndex++;
            }
        }

        return algBuilder;
    }


    AlgBuilder sort( AlgBuilder algBuilder, RexBuilder rexBuilder, List<Pair<RequestColumn, Boolean>> sorts, int limit, int offset ) {
        if ( (sorts == null || sorts.isEmpty()) && (limit >= 0 || offset >= 0) ) {
            algBuilder = algBuilder.limit( offset, limit );
//            log.debug( "Added limit and offset to relation. Session ID: {}.", req.session().id() );
        } else if ( sorts != null && !sorts.isEmpty() ) {
            List<RexNode> sortingNodes = new ArrayList<>();
            AlgNode baseNodeForSorts = algBuilder.peek();
            for ( Pair<RequestColumn, Boolean> sort : sorts ) {
                int inputField = sort.left.getLogicalIndex();
                RexNode inputRef = rexBuilder.makeInputRef( baseNodeForSorts, inputField );
                RexNode sortingNode;
                if ( sort.right ) {
                    RexNode innerNode = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.DESC ), inputRef );
                    sortingNode = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NULLS_FIRST ), innerNode );
                } else {
                    sortingNode = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.NULLS_FIRST ), inputRef );
                }

                sortingNodes.add( sortingNode );
            }

            algBuilder = algBuilder.sortLimit( offset, limit, sortingNodes );
//            log.debug( "Added sort, limit and offset to relation. Session ID: {}.", req.session().id() );
        }

        return algBuilder;
    }


    private Transaction getTransaction() {
        return transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, false, "REST Interface", MultimediaFlavor.FILE );
    }


    String executeAndTransformPolyAlg( AlgRoot algRoot, final Statement statement, final Context ctx ) {
        RestResult restResult;
        try {
            // Prepare
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, true );
            log.debug( "AlgRoot was prepared." );

            final ResultIterator iter = result.execute( statement, 1 );
            restResult = new RestResult( algRoot.kind, iter, result.tupleType, result.getFields() );
            restResult.transform();
            long executionTime = restResult.getExecutionTime();
            if ( !algRoot.kind.belongsTo( Kind.DML ) ) {
                result.getExecutionTimeMonitor().setExecutionTime( executionTime );
            }

            statement.getTransaction().commit();
        } catch ( Throwable e ) {
            log.error( "Error during execution of REST query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
        Pair<String, Integer> result = restResult.getResult( ctx );

        return result.left;
    }

}
