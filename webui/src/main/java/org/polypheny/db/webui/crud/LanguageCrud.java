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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.jetty.websocket.api.Session;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.LanguageType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.cql.Cql2RelConverter;
import org.polypheny.db.cql.CqlQuery;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlUseDatabase;
import org.polypheny.db.processing.MqlProcessor;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.LimitIterator;
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

    private final Crud crud;


    public LanguageCrud( Crud crud ) {
        this.crud = crud;
    }


    public Result processCqlRequest( Session session, QueryRequest request ) {
        try {
            String cqlQueryStr = request.query;
            if ( cqlQueryStr.trim().equals( "" ) ) {
                throw new RuntimeException( "CQL query is an empty string!" );
            }

            CqlParser cqlParser = new CqlParser( cqlQueryStr, "APP" );
            CqlQuery cqlQuery = cqlParser.parse();

            if ( log.isDebugEnabled() ) {
                log.debug( "Starting to process CQL resource request. Session ID: {}.", session );
            }
            //requestCounter.incrementAndGet();
            Transaction transaction = this.crud.getTransaction( request.analyze, request.cache );

            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
            // and in case of auto commit of, the information is overwritten
            InformationManager queryAnalyzer = null;
            if ( request.analyze ) {
                queryAnalyzer = transaction.getQueryAnalyzer().observe( crud );
            }

            Statement statement = transaction.createStatement();
            AlgBuilder algBuilder = AlgBuilder.create( statement );
            JavaTypeFactory typeFactory = transaction.getTypeFactory();
            RexBuilder rexBuilder = new RexBuilder( typeFactory );

            long executionTime = System.nanoTime();

            Cql2RelConverter cql2RelConverter = new Cql2RelConverter( cqlQuery );

            AlgRoot relRoot = cql2RelConverter.convert2Rel( algBuilder, rexBuilder );
            PolyphenyDbSignature<?> signature = statement.getQueryProcessor().prepareQuery( relRoot );

            Result result = getResult( LanguageType.CQL, statement, request, cqlQueryStr, signature, request.noLimit );
            try {
                statement.getTransaction().commit();
            } catch ( TransactionException e ) {
                throw new RuntimeException( "Error while committing.", e );
            }
            executionTime = System.nanoTime() - executionTime;
            if ( queryAnalyzer != null ) {
                Crud.attachQueryAnalyzer( queryAnalyzer, executionTime );
            }

            return result;
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    public List<Result> anyMongoQuery( Session session, QueryRequest request, Crud crud ) {

        Transaction transaction = crud.getTransaction( request.analyze, request.cache );

        PolyphenyDbSignature<?> signature;
        MqlProcessor mqlProcessor = (MqlProcessor) transaction.getProcessor( QueryLanguage.MONGO_QL );
        String mql = request.query;
        Statement statement = transaction.createStatement();

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( crud );
        }

        List<Result> results = new ArrayList<>();

        String[] mqls = mql.trim().split( "\\n(?=(use|db.|show))" );

        String database = request.database;
        long executionTime = System.nanoTime();
        boolean noLimit = false;

        for ( String query : mqls ) {

            MqlNode parsed = (MqlNode) mqlProcessor.parse( query );
            QueryParameters parameters = new MqlQueryParameters( query, database );
            if ( parsed instanceof MqlCollectionStatement && ((MqlCollectionStatement) parsed).getLimit() != null ) {
                noLimit = true;
            }

            if ( parsed.getFamily() == Family.DML && mqlProcessor.needsDdlGeneration( parsed, parameters ) ) {
                mqlProcessor.autoGenerateDDL( crud.getTransaction( request.analyze, request.cache ).createStatement(), parsed, parameters );
            }

            if ( parsed.getFamily() == Family.DDL ) {
                mqlProcessor.prepareDdl( statement, parsed, parameters );
                Result result = new Result( 1 ).setGeneratedQuery( query ).setXid( statement.getTransaction().getXid().toString() );
                results.add( result );
            } else {
                AlgRoot logicalRoot = mqlProcessor.translate( statement, parsed, parameters );

                // Prepare
                signature = statement.getQueryProcessor().prepareQuery( logicalRoot );

                results.add( getResult( LanguageType.MQL, statement, request, query, signature, noLimit ) );
            }

            if ( parsed instanceof MqlUseDatabase ) {
                database = ((MqlUseDatabase) parsed).getDatabase();
            }

            executionTime = System.nanoTime() - executionTime;
            try {
                statement.getTransaction().commit();
                transaction = crud.getTransaction( request.analyze, request.cache );
                statement = transaction.createStatement();
            } catch ( TransactionException e ) {
                throw new RuntimeException( "error while committing" );
            }
        }

        if ( queryAnalyzer != null ) {
            Crud.attachQueryAnalyzer( queryAnalyzer, executionTime );
        }

        return results;
    }


    @NotNull
    private static Result getResult( LanguageType languageType, Statement statement, QueryRequest request, String query, PolyphenyDbSignature<?> signature, final boolean noLimit ) {
        Catalog catalog = Catalog.getInstance();

        Iterator<Object> iterator;
        boolean hasMoreRows;
        List<List<Object>> rows;
        final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
        //noinspection unchecked
        iterator = enumerable.iterator();
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        if ( noLimit ) {
            rows = MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() );
        } else {
            rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, RuntimeConfig.UI_PAGE_SIZE.getInteger() ), new ArrayList<>() );
        }

        hasMoreRows = iterator.hasNext();
        stopWatch.stop();
        signature.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );

        try {
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
            for ( ColumnMetaData metaData : signature.columns ) {
                String columnName = metaData.columnName;

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
                        metaData.columnName,
                        metaData.type.name,
                        metaData.nullable == ResultSetMetaData.columnNullable,
                        metaData.displaySize,
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
                    .setLanguageType( languageType )
                    .setAffectedRows( data.size() )
                    .setHasMoreRows( hasMoreRows )
                    .setGeneratedQuery( query );
        } finally {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( Exception e ) {
                log.error( "Exception while closing result iterator", e );
            }
        }
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
