/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
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
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.restapi.RequestParser.Filters;
import org.polypheny.db.restapi.exception.RestException;
import org.polypheny.db.restapi.models.requests.ResourceDeleteRequest;
import org.polypheny.db.restapi.models.requests.ResourceGetRequest;
import org.polypheny.db.restapi.models.requests.ResourcePatchRequest;
import org.polypheny.db.restapi.models.requests.ResourcePostRequest;
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
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
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


    Map<String, Object> processGetResource( final ResourceGetRequest resourceGetRequest, final Request req, final Response res ) throws RestException {
        log.debug( "Starting to process get resource request. Session ID: {}.", req.session().id() );
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourceGetRequest.tables );

        // Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourceGetRequest.requestColumns );

        List<RexNode> filters = this.filters( relBuilder, rexBuilder, resourceGetRequest.filters, req );
        if ( filters != null ) {
            relBuilder = relBuilder.filter( filters );
        }

        // Aggregates
        relBuilder = this.aggregates( relBuilder, rexBuilder, resourceGetRequest.requestColumns, resourceGetRequest.groupings );

        // Final projection
        relBuilder = this.finalProjection( relBuilder, rexBuilder, resourceGetRequest.requestColumns );

        // Sorts, limit, offset
        relBuilder = this.sort( relBuilder, rexBuilder, resourceGetRequest.sorting, resourceGetRequest.limit, resourceGetRequest.offset );

        log.debug( "RelNodeBuilder: {}", relBuilder.toString() );
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

        Map<String, Object> finalResult = executeAndTransformRelAlg( root, statement );

        finalResult.put( "uri", req.uri() );
        finalResult.put( "query", req.queryString() );
        return finalResult;
    }


    Map<String, Object> processPatchResource( final ResourcePatchRequest resourcePatchRequest, final Request req, final Response res ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourcePatchRequest.tables.get( 0 ).getSchemaName(), resourcePatchRequest.tables.get( 0 ).name ) );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourcePatchRequest.tables );

        // Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourcePatchRequest.requestColumns );

        List<RexNode> filters = this.filters( relBuilder, rexBuilder, resourcePatchRequest.filters, req );
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
        List<RexNode> rexValues = this.valuesNode( relBuilder, rexBuilder, resourcePatchRequest.values, tableRows ).get( 0 );

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

        Map<String, Object> finalResult = executeAndTransformRelAlg( root, statement );
        return finalResult;
    }


    Map<String, Object> processDeleteResource( final ResourceDeleteRequest resourceDeleteRequest, final Request req, final Response res ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( resourceDeleteRequest.tables.get( 0 ).getSchemaName(), resourceDeleteRequest.tables.get( 0 ).name ) );

        // Table Scans
        relBuilder = this.tableScans( relBuilder, rexBuilder, resourceDeleteRequest.tables );

//         Initial projection
        relBuilder = this.initialProjection( relBuilder, rexBuilder, resourceDeleteRequest.requestColumns );

        List<RexNode> filters = this.filters( relBuilder, rexBuilder, resourceDeleteRequest.filters, req );
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

        Map<String, Object> finalResult = executeAndTransformRelAlg( root, statement );
        return finalResult;
    }


    Map<String, Object> processPostResource( final ResourcePostRequest insertValueRequest, final Request req, final Response res ) throws RestException {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        RelBuilder relBuilder = RelBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( insertValueRequest.tables.get( 0 ).getSchemaName(), insertValueRequest.tables.get( 0 ).name ) );

        // Values
        RelDataType tableRowType = table.getRowType();
        List<RelDataTypeField> tableRows = tableRowType.getFieldList();

//        List<String> valueColumnNames = this.valuesColumnNames( updateResourceRequest.values );
        List<List<RexLiteral>> rexValues = this.valuesLiteral( relBuilder, rexBuilder, insertValueRequest.values, tableRows );
        relBuilder = relBuilder.values( rexValues, tableRowType );

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

        Map<String, Object> finalResult = executeAndTransformRelAlg( root, statement );

        return finalResult;
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
    List<RexNode> filters( RelBuilder relBuilder, RexBuilder rexBuilder, Filters filters, Request req ) {
        if ( filters.literalFilters != null ) {
            log.debug( "Starting to process filters. Session ID: {}.", req.session().id() );
            List<RexNode> filterNodes = new ArrayList<>();
            RelNode baseNodeForFilters = relBuilder.peek();
            RelDataType filtersRowType = baseNodeForFilters.getRowType();
            List<RelDataTypeField> filtersRows = filtersRowType.getFieldList();
            for ( RequestColumn column : filters.literalFilters.keySet() ) {
                for ( Pair<SqlOperator, Object> filterOperationPair : filters.literalFilters.get( column ) ) {
                    int columnPosition = (int) column.getLogicalIndex();
                    RelDataTypeField typeField = filtersRows.get( columnPosition );
                    RexNode inputRef = rexBuilder.makeInputRef( baseNodeForFilters, columnPosition );
                    RexNode rightHandSide = rexBuilder.makeLiteral( filterOperationPair.right, typeField.getType(), true );
                    RexNode call = rexBuilder.makeCall( filterOperationPair.left, inputRef, rightHandSide );
                    filterNodes.add( call );
                }
            }

            log.debug( "Finished processing filters. Session ID: {}.", req.session().id() );
//            relBuilder = relBuilder.filter( filterNodes );
            log.debug( "Added filters to relation. Session ID: {}.", req.session().id() );
            return filterNodes;
        } else {
            log.debug( "No filters to add. Session ID: {}.", req.session().id() );
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


    List<List<RexNode>> valuesNode( RelBuilder relBuilder, RexBuilder rexBuilder, List<List<Pair<RequestColumn, Object>>> values, List<RelDataTypeField> tableRows ) {
        List<List<RexNode>> wrapperList = new ArrayList<>();
        for ( List<Pair<RequestColumn, Object>> rowsToInsert : values ) {
            List<RexNode> rexValues = new ArrayList<>();
            for ( Pair<RequestColumn, Object> insertValue : rowsToInsert ) {
                int columnPosition = insertValue.left.getLogicalIndex();
                RelDataTypeField typeField = tableRows.get( columnPosition );
                rexValues.add( rexBuilder.makeLiteral( insertValue.right, typeField.getType(), true ) );
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
            RexNode inputRef = rexBuilder.makeInputRef( baseNode, (int) column.getTableScanIndex() );
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
                RexNode inputRef = rexBuilder.makeInputRef( baseNode, (int) column.getLogicalIndex() );
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
                groupByOrdinals.add( (int) column.getLogicalIndex() );
            }

            GroupKey groupKey = relBuilder.groupKey( ImmutableBitSet.of( groupByOrdinals ) );

            relBuilder = relBuilder.aggregate( groupKey, aggregateCalls );
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
                int inputField = (int) sort.left.getLogicalIndex();
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
            return transactionManager.startTransaction( userName, databaseName, false );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    Map<String, Object> executeAndTransformRelAlg( RelRoot relRoot, final Statement statement ) {
        // Prepare
        PolyphenyDbSignature signature = statement.getQueryProcessor().prepareQuery( relRoot );
        log.debug( "RelRoot was prepared." );

        List<List<Object>> rows;
        try {
            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( statement.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            if ( relRoot.kind.belongsTo( SqlKind.DML ) ) {
                Object object;
                int rowsChanged = -1;
                while ( iterator.hasNext() ) {
                    object = iterator.next();
                    int num;
                    if ( object != null && object.getClass().isArray() ) {
                        Object[] o = (Object[]) object;
                        num = ((Number) o[0]).intValue();
                    } else if ( object != null ) {
                        num = ((Number) object).intValue();
                    } else {
                        throw new RuntimeException( "Result is null" );
                    }
                    // Check if num is equal for all stores
                    if ( rowsChanged != -1 && rowsChanged != num ) {
                        throw new RuntimeException( "The number of changed rows is not equal for all stores!" );
                    }
                    rowsChanged = num;
                }
                rows = new LinkedList<>();
                LinkedList<Object> result = new LinkedList<>();
                result.add( rowsChanged );
                rows.add( result );
            } else {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
                stopWatch.stop();
                signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );
            }
            statement.getTransaction().commit();
        } catch ( Exception | TransactionException e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                transactionException.printStackTrace();
            }
            return null;
        }

        return transformResultIterator( signature, rows );
    }


    Map<String, Object> transformResultIterator( PolyphenyDbSignature<?> signature, List<List<Object>> rows ) {
        List<Map<String, Object>> resultData = new ArrayList<>();

        try {
            /*CatalogTable catalogTable = null;
            if ( request.tableId != null ) {
                String[] t = request.tableId.split( "\\." );
                try {
                    catalogTable = catalog.getTable( this.databaseName, t[0], t[1] );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    log.error( "Caught exception", e );
                }
            }*/
            for ( List<Object> row : rows ) {
                Map<String, Object> temp = new HashMap<>();
                int counter = 0;
                for ( Object o : row ) {
                    if ( signature.rowType.getFieldList().get( counter ).getType().getPolyType().equals( PolyType.TIMESTAMP ) ) {
                        Long nanoSeconds = (Long) o;
                        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond( nanoSeconds / 1000L, (int) ((nanoSeconds % 1000) * 1000), ZoneOffset.UTC );
//                        localDateTime.toString();
                        temp.put( signature.columns.get( counter ).columnName, localDateTime.toString() );
                    } else if ( signature.rowType.getFieldList().get( counter ).getType().getPolyType().equals( PolyType.TIME ) ) {
                        temp.put( signature.columns.get( counter ).columnName, o.toString() );
                    } else if ( signature.rowType.getFieldList().get( counter ).getType().getPolyType().equals( PolyType.TIME ) ) {
                        temp.put( signature.columns.get( counter ).columnName, o.toString() );
                    } else {
                        temp.put( signature.columns.get( counter ).columnName, o );
                    }
                    counter++;
                }
                resultData.add( temp );
            }

        } catch ( Exception e ) {
            log.error( "Something went wrong with the transformation of the result iterator.", e );
            throw new RestException( RestErrorCode.GENERIC );
        }

        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put( "result", resultData );
        finalResult.put( "size", resultData.size() );
        return finalResult;
    }
}
