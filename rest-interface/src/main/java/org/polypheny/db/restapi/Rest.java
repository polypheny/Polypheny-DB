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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
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
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilder.GroupKey;
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
import spark.Request;
import spark.Response;


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


    String processGetResource( final ResourceGetRequest resourceGetRequest, final Request req, final Response res ) throws RestException {
        if ( log.isDebugEnabled() ) {
            log.debug( "Starting to process get resource request. Session ID: {}.", req.session().id() );
        }
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        statement.getTransaction().setMonitoringData( new QueryEvent() );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourceGetRequest.tables );

        // Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourceGetRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, relBuilder, rexBuilder, resourceGetRequest.filters, req );
        if ( filters != null ) {
            relBuilder = relBuilder.filter( filters );
        }

        // Aggregates
        relBuilder = this.aggregates( relBuilder, rexBuilder, resourceGetRequest.requestColumns, resourceGetRequest.groupings );

        // Final projection
        relBuilder = this.finalProjection( relBuilder, rexBuilder, resourceGetRequest.requestColumns );

        // Sorts, limit, offset
        relBuilder = this.sort( relBuilder, rexBuilder, resourceGetRequest.sorting, resourceGetRequest.limit, resourceGetRequest.offset );

        if ( log.isDebugEnabled() ) {
            log.debug( "RelNodeBuilder: {}", relBuilder.toString() );
        }
        RelNode relNode = relBuilder.build();
        log.debug( "RelNode was built." );

        // Wrap RelNode into a RelRoot
        final RelDataType rowType = relNode.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( relNode, relNode.getRowType(), SqlKind.SELECT, fields, collation );
        log.debug( "RelRoot was built." );

        return executeAndTransformRelAlg( root, statement, res );
    }


    String processPatchResource( final ResourcePatchRequest resourcePatchRequest, final Request req, final Response res, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        statement.getTransaction().setMonitoringData( new DmlEvent() );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourcePatchRequest.tables.get( 0 ).getSchemaName(), resourcePatchRequest.tables.get( 0 ).name ) );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourcePatchRequest.tables );

        // Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourcePatchRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, relBuilder, rexBuilder, resourcePatchRequest.filters, req );
        if ( filters != null ) {
            relBuilder = relBuilder.filter( filters );
        }

        // Table Modify

        RelOptPlanner planner = statement.getQueryProcessor().getPlanner();
        RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );

        // Values
        RelDataType tableRowType = table.getRowType();
        List<RelDataTypeField> tableRows = tableRowType.getFieldList();
        List<String> valueColumnNames = this.valuesColumnNames( resourcePatchRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, relBuilder, rexBuilder, resourcePatchRequest, tableRows, inputStreams ).get( 0 );

        RelNode relNode = relBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                relNode.getTraitSet(),
                table,
                catalogReader,
                relNode,
                LogicalTableModify.Operation.UPDATE,
                valueColumnNames,
                rexValues,
                false
        );

        // Wrap RelNode into a RelRoot
        final RelDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( tableModify, rowType, SqlKind.UPDATE, fields, collation );
        log.debug( "RelRoot was built." );

        return executeAndTransformRelAlg( root, statement, res );
    }


    String processDeleteResource( final ResourceDeleteRequest resourceDeleteRequest, final Request req, final Response res ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        statement.getTransaction().setMonitoringData( new DmlEvent() );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourceDeleteRequest.tables.get( 0 ).getSchemaName(), resourceDeleteRequest.tables.get( 0 ).name ) );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourceDeleteRequest.tables );

//         Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourceDeleteRequest.requestColumns );

        List<RexNode> filters = this.filters( statement, relBuilder, rexBuilder, resourceDeleteRequest.filters, req );
        if ( filters != null ) {
            relBuilder = relBuilder.filter( filters );
        }

        // Table Modify

        RelOptPlanner planner = statement.getQueryProcessor().getPlanner();
        RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );

        RelNode relNode = relBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                relNode.getTraitSet(),
                table,
                catalogReader,
                relNode,
                LogicalTableModify.Operation.DELETE,
                null,
                null,
                false
        );

        // Wrap RelNode into a RelRoot
        final RelDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( tableModify, rowType, SqlKind.DELETE, fields, collation );
        log.debug( "RelRoot was built." );

        return executeAndTransformRelAlg( root, statement, res );
    }


    String processPostResource( final ResourcePostRequest insertValueRequest, final Request req, final Response res, Map<String, InputStream> inputStreams ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        statement.getTransaction().setMonitoringData( new DmlEvent() );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( insertValueRequest.tables.get( 0 ).getSchemaName(), insertValueRequest.tables.get( 0 ).name ) );

        // Values
        RelDataType tableRowType = table.getRowType();
        List<RelDataTypeField> tableRows = tableRowType.getFieldList();

//        List<String> valueColumnNames = this.valuesColumnNames( updateResourceRequest.values );

        RelOptPlanner planner = statement.getQueryProcessor().getPlanner();
        RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );

        List<String> valueColumnNames = this.valuesColumnNames( insertValueRequest.values );
        List<RexNode> rexValues = this.valuesNode( statement, relBuilder, rexBuilder, insertValueRequest, tableRows, inputStreams ).get( 0 );
        relBuilder.push( LogicalValues.createOneRow( cluster ) );
        relBuilder.project( rexValues, valueColumnNames );

        // Table Modify
        RelNode relNode = relBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                relNode.getTraitSet(),
                table,
                catalogReader,
                relNode,
                LogicalTableModify.Operation.INSERT,
                null,
                null,
                false
        );

        // Wrap RelNode into a RelRoot
        final RelDataType rowType = tableModify.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( tableModify, rowType, SqlKind.INSERT, fields, collation );
        log.debug( "RelRoot was built." );

        return executeAndTransformRelAlg( root, statement, res );
    }


    @VisibleForTesting
    RelBuilder tableScans( RelBuilder relBuilder, RexBuilder rexBuilder, List<CatalogTable> tables ) {
        boolean firstTable = true;
        for ( CatalogTable catalogTable : tables ) {
            if ( firstTable ) {
                relBuilder = relBuilder.scan( catalogTable.getSchemaName(), catalogTable.name );
                firstTable = false;
            } else {
                relBuilder = relBuilder
                        .scan( catalogTable.getSchemaName(), catalogTable.name )
                        .join( JoinRelType.INNER, rexBuilder.makeLiteral( true ) );
            }
        }
        return relBuilder;
    }


    @VisibleForTesting
    List<RexNode> filters( Statement statement, RelBuilder relBuilder, RexBuilder rexBuilder, Filters filters, Request req ) {
        if ( filters.literalFilters != null ) {
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "Starting to process filters. Session ID: {}.", req.session().id() );
            }
            List<RexNode> filterNodes = new ArrayList<>();
            RelNode baseNodeForFilters = relBuilder.peek();
            RelDataType filtersRowType = baseNodeForFilters.getRowType();
            List<RelDataTypeField> filtersRows = filtersRowType.getFieldList();
            Map<String, RelDataTypeField> filterMap = new HashMap<>();
            filtersRows.forEach( ( r ) -> filterMap.put( r.getKey(), r ) );
            int index = 0;
            for ( RequestColumn column : filters.literalFilters.keySet() ) {
                for ( Pair<SqlOperator, Object> filterOperationPair : filters.literalFilters.get( column ) ) {
                    RelDataTypeField typeField = filterMap.get( column.getFullyQualifiedName() );
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
                log.debug( "Finished processing filters. Session ID: {}.", req.session().id() );
            }
//            relBuilder = relBuilder.filter( filterNodes );
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "Added filters to relation. Session ID: {}.", req.session().id() );
            }
            return filterNodes;
        } else {
            if ( req != null && log.isDebugEnabled() ) {
                log.debug( "No filters to add. Session ID: {}.", req.session().id() );
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


    List<List<RexNode>> valuesNode( Statement statement, RelBuilder relBuilder, RexBuilder rexBuilder, ResourceValuesRequest request, List<RelDataTypeField> tableRows, Map<String, InputStream> inputStreams ) {
        List<List<Pair<RequestColumn, Object>>> values = request.values;
        List<List<RexNode>> wrapperList = new ArrayList<>();
        int index = 0;
        for ( List<Pair<RequestColumn, Object>> rowsToInsert : values ) {
            List<RexNode> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                RelDataTypeField typeField = tableRows.get( columnPosition );
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


    List<List<RexLiteral>> valuesLiteral( RelBuilder relBuilder, RexBuilder rexBuilder, List<List<Pair<RequestColumn, Object>>> values, List<RelDataTypeField> tableRows ) {
        List<List<RexLiteral>> wrapperList = new ArrayList<>();
        for ( List<Pair<RequestColumn, Object>> rowsToInsert : values ) {
            List<RexLiteral> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                RelDataTypeField typeField = tableRows.get( columnPosition );
                rexValues.add( (RexLiteral) rexBuilder.makeLiteral( insertValue.right, typeField.getType(), true ) );
            }
            wrapperList.add( rexValues );
        }

        return wrapperList;
    }


    RelBuilder initialProjection( RelBuilder relBuilder, RexBuilder rexBuilder, List<RequestColumn> columns ) {
        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for ( RequestColumn column : columns ) {
            RexNode inputRef = rexBuilder.makeInputRef( baseNode, column.getTableScanIndex() );
            inputRefs.add( inputRef );
            aliases.add( column.getAlias() );
        }

        relBuilder = relBuilder.project( inputRefs, aliases, true );
        return relBuilder;
    }


    RelBuilder finalProjection( RelBuilder relBuilder, RexBuilder rexBuilder, List<RequestColumn> columns ) {
        RelNode baseNode = relBuilder.peek();
        List<RexNode> inputRefs = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for ( RequestColumn column : columns ) {
            if ( column.isExplicit() ) {
                RexNode inputRef = rexBuilder.makeInputRef( baseNode, column.getLogicalIndex() );
                inputRefs.add( inputRef );
                aliases.add( column.getAlias() );
            }
        }

        relBuilder = relBuilder.project( inputRefs, aliases, true );
        return relBuilder;
    }


    RelBuilder aggregates( RelBuilder relBuilder, RexBuilder rexBuilder, List<RequestColumn> requestColumns, List<RequestColumn> groupings ) {
        RelNode baseNodeForAggregation = relBuilder.peek();

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
                        RelCollations.EMPTY,
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

            GroupKey groupKey = relBuilder.groupKey( ImmutableBitSet.of( groupByOrdinals ) );

            relBuilder = relBuilder.aggregate( groupKey, aggregateCalls );
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

        return relBuilder;
    }


    RelBuilder sort( RelBuilder relBuilder, RexBuilder rexBuilder, List<Pair<RequestColumn, Boolean>> sorts, int limit, int offset ) {
        if ( (sorts == null || sorts.size() == 0) && (limit >= 0 || offset >= 0) ) {
            relBuilder = relBuilder.limit( offset, limit );
//            log.debug( "Added limit and offset to relation. Session ID: {}.", req.session().id() );
        } else if ( sorts != null && sorts.size() != 0 ) {
            List<RexNode> sortingNodes = new ArrayList<>();
            RelNode baseNodeForSorts = relBuilder.peek();
            for ( Pair<RequestColumn, Boolean> sort : sorts ) {
                int inputField = sort.left.getLogicalIndex();
                RexNode inputRef = rexBuilder.makeInputRef( baseNodeForSorts, inputField );
                RexNode sortingNode;
                if ( sort.right ) {
                    RexNode innerNode = rexBuilder.makeCall( SqlStdOperatorTable.DESC, inputRef );
                    sortingNode = rexBuilder.makeCall( SqlStdOperatorTable.NULLS_FIRST, innerNode );
                } else {
                    sortingNode = rexBuilder.makeCall( SqlStdOperatorTable.NULLS_FIRST, inputRef );
                }

                sortingNodes.add( sortingNode );
            }

            relBuilder = relBuilder.sortLimit( offset, limit, sortingNodes );
//            log.debug( "Added sort, limit and offset to relation. Session ID: {}.", req.session().id() );
        }

        return relBuilder;
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false, "REST Interface", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    String executeAndTransformRelAlg( RelRoot relRoot, final Statement statement, final Response res ) {
        RestResult restResult;
        try {
            // Prepare
            PolyphenyDbSignature signature = statement.getQueryProcessor().prepareQuery( relRoot );
            log.debug( "RelRoot was prepared." );

            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( statement.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            restResult = new RestResult( relRoot.kind, iterator, signature.rowType, signature.columns );
            restResult.transform();
            long executionTime = restResult.getExecutionTime();
            if ( !relRoot.kind.belongsTo( SqlKind.DML ) ) {
                signature.getExecutionTimeMonitor().setExecutionTime( executionTime );
            }

            statement.getTransaction().getMonitoringData().setExecutionTime( executionTime );
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
        Pair<String, Integer> result = restResult.getResult( res );
        statement.getTransaction().getMonitoringData().setRowCount( result.right );
        MonitoringServiceProvider.getInstance().monitorEvent( statement.getTransaction().getMonitoringData() );

        return result.left;
    }

}
