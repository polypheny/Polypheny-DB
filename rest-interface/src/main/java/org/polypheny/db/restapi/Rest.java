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


import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
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
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.restapi.models.requests.InsertValueRequest;
import org.polypheny.db.restapi.models.requests.ResourceRequest;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import spark.QueryParamsMap;
import spark.Request;
import spark.Response;

@Slf4j
public class Rest {

    private final Gson gson = new Gson();
    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;

    private final Catalog catalog = Catalog.getInstance();

    Rest( final TransactionManager transactionManager, final String userName, final String databaseName ) {
        this.transactionManager = transactionManager;
        this.userName = userName;
        this.databaseName = databaseName;
    }


    Map<String, Object> getTableList( final Request req, final Response res ) {
        List<CatalogSchema> catalogSchemas;
        try {
            catalogSchemas = catalog.getSchemas( new Pattern( this.databaseName ), null );
        } catch ( GenericCatalogException | UnknownSchemaException e ) {
            e.printStackTrace();
            return null;
        }

        Map<String, List<String>> availableTables = new HashMap<>();
        for ( CatalogSchema catalogSchema : catalogSchemas ) {
            try {
                List<CatalogTable> catalogTables = catalog.getTables( catalogSchema.id, null );
                List<String> tables = new ArrayList<>();
                for ( CatalogTable catalogTable : catalogTables ) {
                    tables.add( catalogTable.name );
                }
                availableTables.put( catalogSchema.name, tables );
            } catch ( GenericCatalogException e ) {
                e.printStackTrace();
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put( "result", availableTables );
        result.put( "uri", req.uri() );
        result.put( "query", req.queryString() );

        return result;
    }


    Map<String, Object> getResourceTable( final ResourceRequest resourceRequest, final Request req, final Response res ) {
        log.debug( "Starting to process resource request. Session ID: {}.", req.session().id() );
        Transaction transaction = getTransaction( false );
//        transaction.resetQueryProcessor();
        RelBuilder relBuilder = RelBuilder.create( transaction );
//        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        // Tables
        log.debug( "Starting to process tables. Session ID: {}.", req.session().id() );
        boolean firstTable = true;
        for ( CatalogTable catalogTable : resourceRequest.tables ) {
            if ( firstTable ) {
                relBuilder = relBuilder.scan( catalogTable.schemaName, catalogTable.name );
                firstTable = false;
            } else {
                relBuilder = relBuilder
                        .scan( catalogTable.schemaName, catalogTable.name )
                        .join( JoinRelType.INNER, rexBuilder.makeLiteral( true ) );
            }
        }
        log.debug( "Finished processing tables. Session ID: {}.", req.session().id() );

        // Filters

        if ( resourceRequest.filters != null ) {
            log.debug( "Starting to process filters. Session ID: {}.", req.session().id() );
            List<RexNode> filterNodes = new ArrayList<>();
            RelNode baseNodeForFilters = relBuilder.peek();
            RelDataType filtersRowType = baseNodeForFilters.getRowType();
            List<RelDataTypeField> filtersRows = filtersRowType.getFieldList();
            for ( CatalogColumn catalogColumn : resourceRequest.filters.keySet() ) {
                for ( Pair<SqlOperator, Object> filterOperationPair : resourceRequest.filters.get( catalogColumn ) ) {
                    int columnPosition = resourceRequest.getInputPosition( catalogColumn );
                    RelDataTypeField typeField = filtersRows.get( columnPosition );
                    RexNode inputRef = rexBuilder.makeInputRef( baseNodeForFilters, columnPosition );
                    RexNode rightHandSide = rexBuilder.makeLiteral( filterOperationPair.right, typeField.getType(), true );
                    RexNode call = rexBuilder.makeCall( filterOperationPair.left, inputRef, rightHandSide );
                    filterNodes.add( call );
                }
            }

            log.debug( "Finished processing filters. Session ID: {}.", req.session().id() );
            relBuilder = relBuilder.filter( filterNodes );
            log.debug( "Added filters to relation. Session ID: {}.", req.session().id() );
        } else {
            log.debug( "No filters to add. Session ID: {}.", req.session().id() );
        }


        // Sorting, Limit and Offset
        if ( ( resourceRequest.sort == null || resourceRequest.sort.size() == 0 ) && ( resourceRequest.limit >= 0 || resourceRequest.offset >= 0 ) ) {
            relBuilder = relBuilder.limit( resourceRequest.offset, resourceRequest.limit );
            log.debug( "Added limit and offset to relation. Session ID: {}.", req.session().id() );
        } else if ( resourceRequest.sort != null && resourceRequest.sort.size() != 0 ) {
            List<RexNode> sortingNodes = new ArrayList<>();
            RelNode baseNodeForSorts = relBuilder.peek();
            for ( Pair<CatalogColumn, Boolean> sort : resourceRequest.sort ) {
                int inputField = resourceRequest.getInputPosition( sort.left );
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

            relBuilder = relBuilder.sortLimit( resourceRequest.offset, resourceRequest.limit, sortingNodes );
            log.debug( "Added sort, limit and offset to relation. Session ID: {}.", req.session().id() );
        } else {
            log.debug( "No sort, limit, or offset to add. Session ID: {}.", req.session().id() );
        }


        // Projections
        if ( resourceRequest.projection != null ) {
            List<RexNode> projectionInputRefs = new ArrayList<>();
            RelNode baseNodeForProjections = relBuilder.peek();
            for ( CatalogColumn catalogColumn : resourceRequest.projection.left ) {
                int inputField = resourceRequest.getInputPosition( catalogColumn );
                RexNode inputRef = rexBuilder.makeInputRef( baseNodeForProjections, inputField );
                projectionInputRefs.add( inputRef );
            }

            relBuilder = relBuilder.project( projectionInputRefs, resourceRequest.projection.right );
            log.debug( "Added projections to relation. Session ID: {}.", req.session().id() );
        } else {
            log.debug( "No projections to add. Session ID: {}.", req.session().id() );
        }

        // Limit and Offset
        /*if ( resourceRequest.limit >= 0 || resourceRequest.offset >= 0 ) {
            relBuilder = relBuilder.limit( resourceRequest.offset, resourceRequest.limit );
        }*/


        RelNode result = relBuilder.build();

        Map<String, Object> finalResult = executeAndTransformRelAlg( result, transaction );

//        finalResult.put( "uri", req.uri() );
//        finalResult.put( "query", req.queryString() );
        return finalResult;
    }


    Map<String, Object> postInsertValue( InsertValueRequest insertValueRequest, final Request req, final Response res ) {
        log.debug( "Starting to process insert value request. Session ID: {}.", req.session().id() );
        Transaction transaction = getTransaction( false );
        RelBuilder relBuilder = RelBuilder.create( transaction );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        // RelOptTable
        PolyphenyDbCatalogReader catalogReader = transaction.getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( insertValueRequest.table.schemaName, insertValueRequest.table.name ) );

        // Values
        RelDataType tableRowType = table.getRowType();
        List<RelDataTypeField> tableRows = tableRowType.getFieldList();

        List<RexLiteral> rexValues = new ArrayList<>();
        List<Object> actualRexValues = new ArrayList<>();
        for ( Pair<CatalogColumn, Object> insertValue : insertValueRequest.values ) {
            int columnPosition = insertValueRequest.getInputPosition( insertValue.left );
            RelDataTypeField typeField = tableRows.get( columnPosition );
            rexValues.add( (RexLiteral) rexBuilder.makeLiteral( insertValue.right, typeField.getType(), true ) );
            actualRexValues.add( insertValue.right );
        }

//        relBuilder = relBuilder.values( tableRowType, actualRexValues );
        List<List<RexLiteral>> wrapperList = new ArrayList<>();
        wrapperList.add( rexValues );
        relBuilder = relBuilder.values( wrapperList, tableRowType );

        // Table Modify

        RelOptPlanner planner = transaction.getQueryProcessor().getPlanner();
        RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );

        RelNode plan = relBuilder.build();
        TableModify tableModify = new LogicalTableModify(
                cluster,
                plan.getTraitSet(),
                table,
                catalogReader,
                plan,
                LogicalTableModify.Operation.INSERT,
                null,
                null,
                false
        );

        Map<String, Object> finalResult = executeAndTransformRelAlg( tableModify, transaction );

        return finalResult;
    }


    Map<String, Object> getTable( final Request req, final Response res ) {
        String tableName = req.params( ":resName" );
        log.info( "Tables param: {}", tableName );
        Transaction transaction = getTransaction( true );
        transaction.resetQueryProcessor();
        log.info( "Transaction prepared." );

        String[] tables = tableName.split( "," );

        RelBuilder relBuilder = RelBuilder.create( transaction );

        boolean firstTable = true;
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );
        log.info( "Tables: {}", tables.length );
        for (String table: tables) {
            if ( firstTable ) {
                relBuilder = relBuilder.scan( table.split( "\\." ) );
                LogicalTableScan tableScan = (LogicalTableScan) relBuilder.peek();
                Table relAlgTable = ((RelOptTableImpl) tableScan.getTable()).getTable();
                for ( String possibleFilter : req.queryMap().toMap().keySet() ) {
                    if ( possibleFilter.startsWith( table ) ) {
                        String[] splitPossibleFilter = possibleFilter.split( "\\." );
                        int counter = 0;
                        for ( RelDataTypeField relDataTypeField : relAlgTable.getRowType( typeFactory ).getFieldList() ) {
                            if ( relDataTypeField.getName().equals( splitPossibleFilter[2] ) ) {
                                RelDataType relDataType = relDataTypeField.getType();
                                QueryParamsMap toCompute = req.queryMap( possibleFilter );
                                for ( String valuesBlaBla : toCompute.values() ) {
                                    RexNode inputRef = rexBuilder.makeInputRef( tableScan, counter );
                                    SqlOperator callOperator;
                                    String restOfOp;
                                    if ( valuesBlaBla.startsWith( "<" ) ) {
                                        callOperator = SqlStdOperatorTable.LESS_THAN;
                                        restOfOp = valuesBlaBla.substring( 1, valuesBlaBla.length() );
                                    } else if ( valuesBlaBla.startsWith( "<=" ) ) {
                                        callOperator = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
                                        restOfOp = valuesBlaBla.substring( 2, valuesBlaBla.length() );
                                    } else if ( valuesBlaBla.startsWith( ">" ) ) {
                                        callOperator = SqlStdOperatorTable.GREATER_THAN;
                                        restOfOp = valuesBlaBla.substring( 1, valuesBlaBla.length() );
                                    } else if ( valuesBlaBla.startsWith( ">=" ) ) {
                                        callOperator = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
                                        restOfOp = valuesBlaBla.substring( 2, valuesBlaBla.length() );
                                    } else if ( valuesBlaBla.startsWith( "=" ) ) {
                                        callOperator = SqlStdOperatorTable.EQUALS;
                                        restOfOp = valuesBlaBla.substring( 1, valuesBlaBla.length() );
                                    } else {
                                        log.error( "bugger me..." );
                                        return null;
                                    }
//                                    PolyTypeFactoryImpl polyTypeFactory = new PolyTypeFactoryImpl( relDataType );
                                    RexNode rightSide = rexBuilder.makeLiteral( Integer.valueOf( restOfOp ), relDataType, true );
                                    RexNode call = rexBuilder.makeCall( callOperator, inputRef, rightSide );
                                    relBuilder = relBuilder.filter( call );
                                }
//                                RexNode constantValue = rexBuilder
//                                rexBuilder.makeFieldAccess( tableScan, "bla", false )
                            }
                            counter++;
                        }
                    }
                }

                firstTable = false;
            } else {
                relBuilder = relBuilder.scan( table.split( "\\." ) );
                relBuilder = relBuilder.join( JoinRelType.INNER, rexBuilder.makeLiteral( true ) );
            }
        }
        log.info( "RelNodeBuilder: {}", relBuilder.toString() );
        RelNode result = relBuilder.build();
        log.info( "RelNode was built." );

        Map<String, Object> finalResult = executeAndTransformRelAlg( result, transaction );

        finalResult.put( "uri", req.uri() );
        finalResult.put( "query", req.queryString() );
        return finalResult;
    }

    Map<String, Object> executeAndTransformRelAlg( RelNode relNode, final Transaction transaction ) {
        // Wrap RelNode into a RelRoot
        final RelDataType rowType = relNode.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                relNode instanceof Sort
                        ? ((Sort) relNode).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( relNode, relNode.getRowType(), SqlKind.SELECT, fields, collation );
        log.info( "RelRoot was built." );

        // Prepare
        PolyphenyDbSignature signature = transaction.getQueryProcessor().prepareQuery( root );
        log.info( "RelRoot was prepared." );

        List<List<Object>> rows;
        try {
            @SuppressWarnings("unchecked") final Iterable<Object> iterable = signature.enumerable( transaction.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
            stopWatch.stop();
            if ( root.kind.belongsTo( SqlKind.DML ) ) {
                signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );
            }
            transaction.commit();
        } catch ( Exception | TransactionException e ) {
            log.error( "Caught exception while iterating the plan builder tree", e );
            try {
                transaction.rollback();
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
                for ( Object o: row ) {
                    temp.put( signature.columns.get( counter ).columnName, o );
                    counter++;
                }
                resultData.add( temp );
            }

        } catch ( Exception e ) {

        }

        Map<String, Object> finalResult = new HashMap<>();
        finalResult.put( "result", resultData );
        finalResult.put( "size", resultData.size() );
        return finalResult;
    }

    private Transaction getTransaction() {
        return getTransaction( false );
    }

    private Transaction getTransaction( boolean analyze ) {
        try {
            return transactionManager.startTransaction( userName, databaseName, analyze );
        } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    Object testMethod( final Request req, final Response res ) {
        log.info( "Something arrived here!" );

        Transaction transaction = this.getTransaction();
        PolyphenyDbCatalogReader catalogReader = transaction.getCatalogReader();
        PreparingTable table = catalogReader.getTable( Arrays.asList( "public", "emps" ) );
        log.info( "blabla" );

//        SqlValidatorUtil.getRelOptTable(  )
        // DEMO THINGS!

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            e.printStackTrace();
        }

        return null;
    }
}
