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

package org.polypheny.db.restapi;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.javalin.http.Context;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyResult;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.LogicalTableModify;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
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
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.FileInputHandle;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;


@Slf4j
public class Rest {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    Rest( final TransactionManager transactionManager, final String userName, final String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
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

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = algNode.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( algNode, algNode.getRowType(), Kind.SELECT, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    String processPatchResource( final ResourcePatchRequest resourcePatchRequest, final Context ctx, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourcePatchRequest.tables.get( 0 ).getSchemaName(), resourcePatchRequest.tables.get( 0 ).name ) );

        // Table Scans
        algBuilder = this.tableScans( algBuilder, rexBuilder, resourcePatchRequest.tables );

        // Initial projection
        algBuilder = this.initialProjection( algBuilder, rexBuilder, resourcePatchRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, algBuilder, rexBuilder, resourcePatchRequest.filters, ctx.req );
        if ( filters != null ) {
            algBuilder = algBuilder.filter( filters );
        }

        // Table Modify

        AlgOptPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );

        // Values
        AlgDataType tableRowType = table.getRowType();
        List<AlgDataTypeField> tableRows = tableRowType.getFieldList();
        List<String> valueColumnNames = this.valuesColumnNames( resourcePatchRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, algBuilder, rexBuilder, resourcePatchRequest, tableRows, inputStreams ).get( 0 );

        AlgNode algNode = algBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                algNode.getTraitSet(),
                table,
                catalogReader,
                algNode,
                LogicalTableModify.Operation.UPDATE,
                valueColumnNames,
                rexValues,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( tableModify, rowType, Kind.UPDATE, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    String processDeleteResource( final ResourceDeleteRequest resourceDeleteRequest, final Context ctx ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourceDeleteRequest.tables.get( 0 ).getSchemaName(), resourceDeleteRequest.tables.get( 0 ).name ) );

        // Table Scans
        algBuilder = this.tableScans( algBuilder, rexBuilder, resourceDeleteRequest.tables );

//         Initial projection
        algBuilder = this.initialProjection( algBuilder, rexBuilder, resourceDeleteRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, algBuilder, rexBuilder, resourceDeleteRequest.filters, ctx.req );
        if ( filters != null ) {
            algBuilder = algBuilder.filter( filters );
        }

        // Table Modify

        AlgOptPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );

        AlgNode algNode = algBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                algNode.getTraitSet(),
                table,
                catalogReader,
                algNode,
                LogicalTableModify.Operation.DELETE,
                null,
                null,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( tableModify, rowType, Kind.DELETE, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    String processPostResource( final ResourcePostRequest insertValueRequest, final Context ctx, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( insertValueRequest.tables.get( 0 ).getSchemaName(), insertValueRequest.tables.get( 0 ).name ) );

        // Values
        AlgDataType tableRowType = table.getRowType();
        List<AlgDataTypeField> tableRows = tableRowType.getFieldList();

//        List<String> valueColumnNames = this.valuesColumnNames( updateResourceRequest.values );

        AlgOptPlanner planner = statement.getQueryProcessor().getPlanner();
        AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );

        List<String> valueColumnNames = this.valuesColumnNames( insertValueRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, algBuilder, rexBuilder, insertValueRequest, tableRows, inputStreams ).get( 0 );
        algBuilder.push( LogicalValues.createOneRow( cluster ) );
        algBuilder.project( rexValues, valueColumnNames );

        // Table Modify
        AlgNode algNode = algBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                algNode.getTraitSet(),
                table,
                catalogReader,
                algNode,
                LogicalTableModify.Operation.INSERT,
                null,
                null,
                false
        );

        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( tableModify, rowType, Kind.INSERT, fields, collation );
        log.debug( "AlgRoot was built." );

        return executeAndTransformPolyAlg( root, statement, ctx );
    }


    @VisibleForTesting
    AlgBuilder tableScans( AlgBuilder algBuilder, RexBuilder rexBuilder, List<CatalogTable> tables ) {
        boolean firstTable = true;
        for ( CatalogTable catalogTable : tables ) {
            if ( firstTable ) {
                algBuilder = algBuilder.scan( catalogTable.getSchemaName(), catalogTable.name );
                firstTable = false;
            } else {
                algBuilder = algBuilder
                        .scan( catalogTable.getSchemaName(), catalogTable.name )
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
            AlgDataType filtersRowType = baseNodeForFilters.getRowType();
            List<AlgDataTypeField> filtersRows = filtersRowType.getFieldList();
            Map<String, AlgDataTypeField> filterMap = new HashMap<>();
            filtersRows.forEach( ( r ) -> filterMap.put( r.getKey(), r ) );
            int index = 0;
            for ( RequestColumn column : filters.literalFilters.keySet() ) {
                for ( Pair<Operator, Object> filterOperationPair : filters.literalFilters.get( column ) ) {
                    AlgDataTypeField typeField = filterMap.get( column.getFullyQualifiedName() );
                    RexNode inputRef = rexBuilder.makeInputRef( baseNodeForFilters, typeField.getIndex() );
                    Object param = filterOperationPair.right;
                    if ( param instanceof TimestampString ) {
                        param = ((TimestampString) param).toCalendar();
                    } else if ( param instanceof TimeString ) {
                        param = ((TimeString) param).toCalendar();
                    } else if ( param instanceof DateString ) {
                        param = ((DateString) param).toCalendar();
                    }
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
//            algBuilder = algBuilder.filter( filterNodes );
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


    List<String> valuesColumnNames( List<List<Pair<RequestColumn, Object>>> values ) {
        List<String> valueColumnNames = new ArrayList<>();
        List<Pair<RequestColumn, Object>> rowsToInsert = values.get( 0 );
        for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
            valueColumnNames.add( insertValue.left.getColumn().name );
        }

        return valueColumnNames;
    }


    List<List<RexNode>> valuesNode( Statement statement, AlgBuilder algBuilder, RexBuilder rexBuilder, ResourceValuesRequest request, List<AlgDataTypeField> tableRows, Map<String, InputStream> inputStreams ) {
        List<List<Pair<RequestColumn, Object>>> values = request.values;
        List<List<RexNode>> wrapperList = new ArrayList<>();
        int index = 0;
        for ( List<Pair<RequestColumn, Object>> rowsToInsert : values ) {
            List<RexNode> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                AlgDataTypeField typeField = tableRows.get( columnPosition );
                if ( inputStreams != null && request.useDynamicParams && typeField.getType().getPolyType().getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                    FileInputHandle fih = new FileInputHandle( statement, inputStreams.get( insertValue.left.getColumn().name ) );
                    statement.getDataContext().addParameterValues( index, typeField.getType(), ImmutableList.of( fih ) );
                    rexValues.add( rexBuilder.makeDynamicParam( typeField.getType(), index ) );
                    index++;
                } else {
                    rexValues.add( rexBuilder.makeLiteral( insertValue.right, typeField.getType(), true ) );
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
            RexNode inputRef = rexBuilder.makeInputRef( baseNode, column.getTableScanIndex() );
            inputRefs.add( inputRef );
            aliases.add( column.getAlias() );
        }

        algBuilder = algBuilder.project( inputRefs, aliases, true );
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
                        (Operator & AggFunction) column.getAggregate(),
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
        if ( (sorts == null || sorts.size() == 0) && (limit >= 0 || offset >= 0) ) {
            algBuilder = algBuilder.limit( offset, limit );
//            log.debug( "Added limit and offset to relation. Session ID: {}.", req.session().id() );
        } else if ( sorts != null && sorts.size() != 0 ) {
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
        try {
            return transactionManager.startTransaction( userName, databaseName, false, "REST Interface", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    String executeAndTransformPolyAlg( AlgRoot algRoot, final Statement statement, final Context ctx ) {
        RestResult restResult;
        try {
            // Prepare
            PolyResult result = statement.getQueryProcessor().prepareQuery( algRoot, true );
            log.debug( "AlgRoot was prepared." );

            final Iterable<Object> iterable = result.enumerable( statement.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            restResult = new RestResult( algRoot.kind, iterator, result.rowType, result.getColumns() );
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
