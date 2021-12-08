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

package org.polypheny.db.webui.crud;

import com.google.gson.Gson;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyResult;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.cql.Cql2RelConverter;
import org.polypheny.db.cql.CqlQuery;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlUseDatabase;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.Crud.QueryExecutionException;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.requests.EditCollectionRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import spark.Request;
import spark.Response;

@Slf4j
public class LanguageCrud {

    @Getter
    private static Crud crud;


    public LanguageCrud( Crud crud ) {
        LanguageCrud.crud = crud;
    }


    public static List<Result> anyQuery(
            QueryLanguage language,
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            String userName,
            String databaseName,
            InformationObserver observer ) {
        List<Result> results;

        switch ( language ) {
            case CQL:
                results = processCqlRequest( session, request, transactionManager, userName, databaseName, observer );
                break;
            case MONGO_QL:
                results = anyMongoQuery( session, request, transactionManager, userName, databaseName, observer );
                break;
            case SQL:
                results = LanguageCrud.crud.anySqlQuery( request, session );
                break;
            case PIG:
                results = anyPigQuery( session, request, transactionManager, userName, databaseName, observer );
                break;
            default:
                return Collections.singletonList( new Result( "The used language seems not to be supported!" ) );
        }

        return results;
    }


    public static List<Result> anyPigQuery(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            String userName,
            String databaseName,
            InformationObserver observer ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userName, databaseName );
        try {
            if ( query.trim().equals( "" ) ) {
                throw new RuntimeException( "PIG query is an empty string!" );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Starting to process PIG resource request. Session ID: {}.", session );
            }

            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
            // and in case of auto commit of, the information is overwritten
            InformationManager queryAnalyzer = null;
            if ( request.analyze ) {
                queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
            }

            Statement statement = transaction.createStatement();

            long executionTime = System.nanoTime();
            Processor processor = transaction.getProcessor( QueryLanguage.PIG );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            Node parsed = processor.parse( query );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }
            AlgRoot algRoot = processor.translate( statement, parsed, new QueryParameters( query, SchemaType.RELATIONAL ) );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Translation" );
            }

            PolyResult signature = statement.getQueryProcessor().prepareQuery( algRoot, true );

            Result result = getResult( QueryLanguage.PIG, statement, request, query, signature, request.noLimit );

            String commitStatus;
            try {
                transaction.commit();
                commitStatus = "Committed";
            } catch ( TransactionException e ) {
                log.error( "Error while committing", e );
                try {
                    transaction.rollback();
                    commitStatus = "Rolled back";
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", e );
                    commitStatus = "Error while rolling back";
                }
            }

            executionTime = System.nanoTime() - executionTime;
            if ( queryAnalyzer != null ) {
                Crud.attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, 1 );
            }

            return Collections.singletonList( result );
        } catch ( Throwable t ) {
            return Collections.singletonList( new Result( t ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() ) );
        }
    }


    public static List<Result> processCqlRequest(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            String userName,
            String databaseName,
            InformationObserver observer ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userName, databaseName );
        try {
            if ( query.trim().equals( "" ) ) {
                throw new RuntimeException( "CQL query is an empty string!" );
            }

            if ( log.isDebugEnabled() ) {
                log.debug( "Starting to process CQL resource request. Session ID: {}.", session );
            }

            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
            // and in case of auto commit of, the information is overwritten
            InformationManager queryAnalyzer = null;
            if ( request.analyze ) {
                queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
            }

            Statement statement = transaction.createStatement();
            AlgBuilder algBuilder = AlgBuilder.create( statement );
            JavaTypeFactory typeFactory = transaction.getTypeFactory();
            RexBuilder rexBuilder = new RexBuilder( typeFactory );

            long executionTime = System.nanoTime();

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            CqlParser cqlParser = new CqlParser( query, "APP" );
            CqlQuery cqlQuery = cqlParser.parse();
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }
            Cql2RelConverter cql2AlgConverter = new Cql2RelConverter( cqlQuery );
            AlgRoot algRoot = cql2AlgConverter.convert2Rel( algBuilder, rexBuilder );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }

            PolyResult signature = statement.getQueryProcessor().prepareQuery( algRoot, true );

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Execution" );
            }
            Result result = getResult( QueryLanguage.CQL, statement, request, query, signature, request.noLimit );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Execution" );
            }

            String commitStatus;
            try {
                transaction.commit();
                commitStatus = "Committed";
            } catch ( TransactionException e ) {
                log.error( "Error while committing", e );
                try {
                    transaction.rollback();
                    commitStatus = "Rolled back";
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", e );
                    commitStatus = "Error while rolling back";
                }
            }

            executionTime = System.nanoTime() - executionTime;
            if ( queryAnalyzer != null ) {
                Crud.attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, 1 );
            }

            return Collections.singletonList( result );
        } catch ( Throwable t ) {
            return Collections.singletonList( new Result( t ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() ) );
        }
    }


    public static List<Result> anyMongoQuery(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            String userName,
            String databaseName,
            InformationObserver observer ) {

        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userName, databaseName );
        PolyResult signature;
        MqlProcessor mqlProcessor = (MqlProcessor) transaction.getProcessor( QueryLanguage.MONGO_QL );
        String mql = request.query;

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
        }

        List<Result> results = new ArrayList<>();

        String[] mqls = mql.trim().split( "\\n(?=(use|db.|show))" );

        String database = request.database;
        long executionTime = System.nanoTime();
        boolean noLimit = false;

        for ( String query : mqls ) {
            Statement statement = transaction.createStatement();
            QueryParameters parameters = new MqlQueryParameters( query, database, SchemaType.DOCUMENT );

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            MqlNode parsed = (MqlNode) mqlProcessor.parse( query );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

            if ( parsed instanceof MqlUseDatabase ) {
                database = ((MqlUseDatabase) parsed).getDatabase();
                //continue;
            }

            if ( parsed instanceof MqlCollectionStatement && ((MqlCollectionStatement) parsed).getLimit() != null ) {
                noLimit = true;
            }

            if ( parsed.getFamily() == Family.DML && mqlProcessor.needsDdlGeneration( parsed, parameters ) ) {
                mqlProcessor.autoGenerateDDL( Crud.getTransaction( request.analyze, request.cache, transactionManager, userName, databaseName ).createStatement(), parsed, parameters );
            }

            if ( parsed.getFamily() == Family.DDL ) {
                mqlProcessor.prepareDdl( statement, parsed, parameters );
                Result result = new Result( 1 ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() );
                results.add( result );
            } else {
                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().start( "Translation" );
                }
                AlgRoot logicalRoot = mqlProcessor.translate( statement, parsed, parameters );
                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().stop( "Translation" );
                }

                // Prepare
                signature = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().start( "Execution" );
                }
                results.add( getResult( QueryLanguage.MONGO_QL, statement, request, query, signature, noLimit ) );
                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().stop( "Execution" );
                }
            }
        }

        executionTime = System.nanoTime() - executionTime;
        String commitStatus;
        try {
            transaction.commit();
            commitStatus = "Committed";
        } catch ( TransactionException e ) {
            log.error( "Caught exception", e );
            results.add( new Result( e ) );
            try {
                transaction.rollback();
                commitStatus = "Rolled back";
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
                commitStatus = "Error while rolling back";
            }
        }

        if ( queryAnalyzer != null ) {
            Crud.attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, results.size() );
        }

        return results;
    }


    @NotNull
    public static Result getResult( QueryLanguage language, Statement statement, QueryRequest request, String query, PolyResult signature, final boolean noLimit ) {
        Catalog catalog = Catalog.getInstance();

        List<List<Object>> rows = signature.getRows( statement, noLimit ? -1 : RuntimeConfig.UI_PAGE_SIZE.getInteger() );
        boolean hasMoreRows = signature.hasMoreRows();

        CatalogTable catalogTable = null;
        if ( request.tableId != null ) {
            String[] t = request.tableId.split( "\\." );
            try {
                catalogTable = Catalog.getInstance().getTable( statement.getPrepareContext().getDefaultSchemaName(), t[0], t[1] );
            } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                log.error( "Caught exception", e );
            }
        }

        ArrayList<DbColumn> header = new ArrayList<>();
        for ( AlgDataTypeField metaData : signature.rowType.getFieldList() ) {
            String columnName = metaData.getName();

            String filter = "";
            if ( request.filter != null && request.filter.containsKey( columnName ) ) {
                filter = request.filter.get( columnName );
            }

            SortState sort;
            if ( request.sortState != null && request.sortState.containsKey( columnName ) ) {
                sort = request.sortState.get( columnName );
            } else {
                sort = new SortState();
            }

            DbColumn dbCol = new DbColumn(
                    metaData.getName(),
                    metaData.getType().getFullTypeString(),
                    metaData.getType().isNullable() == (ResultSetMetaData.columnNullable == 1),
                    metaData.getType().getPrecision(),
                    sort,
                    filter );

            // Get column default values
            if ( catalogTable != null ) {
                try {
                    if ( catalog.checkIfExistsColumn( catalogTable.id, columnName ) ) {
                        CatalogColumn catalogColumn = catalog.getColumn( catalogTable.id, columnName );
                        if ( catalogColumn.defaultValue != null ) {
                            dbCol.defaultValue = catalogColumn.defaultValue.value;
                        }
                    }
                } catch ( UnknownColumnException e ) {
                    log.error( "Caught exception", e );
                }
            }
            header.add( dbCol );
        }

        ArrayList<String[]> data = Crud.computeResultData( rows, header, statement.getTransaction() );

        return new Result( header.toArray( new DbColumn[0] ), data.toArray( new String[0][] ) )
                .setSchemaType( signature.getSchemaType() )
                .setLanguage( language )
                .setAffectedRows( data.size() )
                .setHasMoreRows( hasMoreRows )
                .setGeneratedQuery( query );
    }


    /**
     * Creates a new document collection
     *
     * @param req the request, which contains the definition of the collection creation
     * @param res the response for the creation
     * @return a Result object, which contains if the creation was successful
     */
    public Result createCollection( final Request req, final Response res ) {
        EditCollectionRequest request = new Gson().fromJson( req.body(), EditCollectionRequest.class );
        Transaction transaction = this.crud.getTransaction();
        StringBuilder query = new StringBuilder();
        StringJoiner colBuilder = new StringJoiner( "," );
        String tableId = String.format( "\"%s\".\"%s\"", request.database, request.collection );
        query.append( "CREATE TABLE " ).append( tableId ).append( "(" );
        Result result;
        colBuilder.add( "\"_id\" VARCHAR(24) NOT NULL" );
        //colBuilder.add( "\"_data\" JSON" );
        StringJoiner primaryKeys = new StringJoiner( ",", "PRIMARY KEY (", ")" );
        primaryKeys.add( "\"_id\"" );
        colBuilder.add( primaryKeys.toString() );
        query.append( colBuilder );
        query.append( ")" );
        if ( request.store != null && !request.store.equals( "" ) ) {
            query.append( String.format( " ON STORE \"%s\"", request.store ) );
        }

        try {
            int a = this.crud.executeSqlUpdate( transaction, query.toString() );
            result = new Result( a ).setGeneratedQuery( query.toString() );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = new Result( e ).setGeneratedQuery( query.toString() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback CREATE TABLE statement: {}", ex.getMessage(), ex );
            }
        }
        return result;
    }


    /**
     * This query returns a list of all available document databases (Polypheny schema),
     * as a query result
     */
    public Result getDocumentDatabases( Request request, Response response ) {
        Map<String, String> names = Catalog.getInstance()
                .getSchemas( Catalog.defaultDatabaseId, null )
                .stream()
                .collect( Collectors.toMap( CatalogSchema::getName, s -> s.schemaType.name() ) );

        String[][] data = names.entrySet().stream().map( n -> new String[]{ n.getKey(), n.getValue() } ).toArray( String[][]::new );
        return new Result( new DbColumn[]{ new DbColumn( "Database/Schema" ), new DbColumn( "Type" ) }, data );
    }

}
