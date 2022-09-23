/*
 * Copyright 2019-2022 The Polypheny Project
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

import io.javalin.http.Context;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownCollectionException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.cql.Cql2RelConverter;
import org.polypheny.db.cql.CqlQuery;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.cypher.CypherStatement;
import org.polypheny.db.cypher.cypher2alg.CypherQueryParameters;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Family;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.MqlQueryParameters;
import org.polypheny.db.languages.mql.MqlUseDatabase;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.AutomaticDdlProcessor;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.DbColumn;
import org.polypheny.db.webui.models.Index;
import org.polypheny.db.webui.models.Placement;
import org.polypheny.db.webui.models.Placement.DocumentStore;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.requests.EditCollectionRequest;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.UIRequest;

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
            long userId,
            long databaseId,
            InformationObserver observer ) {
        List<Result> results;

        switch ( language ) {
            case CQL:
                results = processCqlRequest( session, request, transactionManager, userId, databaseId, observer );
                break;
            case MONGO_QL:
                results = anyMongoQuery( session, request, transactionManager, userId, databaseId, observer );
                break;
            case SQL:
                results = LanguageCrud.crud.anySqlQuery( request, session );
                break;
            case PIG:
                results = anyPigQuery( session, request, transactionManager, userId, databaseId, observer );
                break;
            case CYPHER:
                results = anyCypherQuery( session, request, transactionManager, userId, databaseId, observer );
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
            long userId,
            long databaseId,
            InformationObserver observer ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface Pig" );
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
            Node parsed = processor.parse( query ).get( 0 );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Translation" );
            }
            AlgRoot algRoot = processor.translate( statement, parsed, new QueryParameters( query, NamespaceType.RELATIONAL ) );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Translation" );
            }

            PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( algRoot, true );

            Result result = getResult( QueryLanguage.PIG, statement, request, query, polyImplementation, transaction, request.noLimit );

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
            long userId,
            long databaseId,
            InformationObserver observer ) {
        String query = request.query;
        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface CQL" );
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

            PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( algRoot, true );

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Execution" );
            }
            Result result = getResult( QueryLanguage.CQL, statement, request, query, polyImplementation, transaction, request.noLimit );
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
            long userId,
            long databaseId,
            InformationObserver observer ) {

        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface MQL" );
        AutomaticDdlProcessor mqlProcessor = (AutomaticDdlProcessor) transaction.getProcessor( QueryLanguage.MONGO_QL );

        if ( request.analyze ) {
            transaction.getQueryAnalyzer().setSession( session );
        }

        InformationManager queryAnalyzer = attachAnalyzerIfSpecified( request, observer, transaction );

        List<Result> results = new ArrayList<>();

        String[] mqls = request.query.trim().split( "\\n(?=(use|db.|show))" );

        String database = request.database;
        long executionTime = System.nanoTime();
        boolean noLimit = false;

        for ( String query : mqls ) {
            try {
                Statement statement = transaction.createStatement();
                QueryParameters parameters = new MqlQueryParameters( query, database, NamespaceType.DOCUMENT );

                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().start( "Parsing" );
                }
                MqlNode parsed = (MqlNode) mqlProcessor.parse( query ).get( 0 );
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
                    mqlProcessor.autoGenerateDDL( Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface MQL (auto)" ).createStatement(), parsed, parameters );
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
                    PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Execution" );
                    }
                    results.add( getResult( QueryLanguage.MONGO_QL, statement, request, query, polyImplementation, transaction, noLimit ).setNamespaceType( NamespaceType.DOCUMENT ) );
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Execution" );
                    }
                }
            } catch ( Throwable t ) {
                attachError( transaction, results, query, t );
            }
        }

        commitAndFinish( transaction, queryAnalyzer, results, executionTime );

        return results;
    }


    private static void commitAndFinish( Transaction transaction, InformationManager queryAnalyzer, List<Result> results, long executionTime ) {
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
    }


    @Nullable
    private static InformationManager attachAnalyzerIfSpecified( QueryRequest request, InformationObserver observer, Transaction transaction ) {
        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( request.analyze ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
        }
        return queryAnalyzer;
    }


    public static PolyGraph getGraph( String databaseName, TransactionManager manager ) {

        Transaction transaction = Crud.getTransaction( false, false, manager, Catalog.defaultUserId, Catalog.defaultDatabaseId, "getGraph" );
        Processor processor = transaction.getProcessor( QueryLanguage.CYPHER );
        Statement statement = transaction.createStatement();

        CypherQueryParameters parameters = new CypherQueryParameters( databaseName );
        AlgRoot logicalRoot = processor.translate( statement, null, parameters );
        PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

        List<List<Object>> res = polyImplementation.getRows( statement, 1 );

        try {
            statement.getTransaction().commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( "Error while committing graph retrieval query." );
        }

        return (PolyGraph) res.get( 0 ).get( 0 );
    }


    public static List<Result> anyCypherQuery(
            Session session,
            QueryRequest request,
            TransactionManager transactionManager,
            long userId,
            long databaseId,
            InformationObserver observer ) {

        String query = request.query;

        Transaction transaction = Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface Cypher" );
        AutomaticDdlProcessor cypherProcessor = (AutomaticDdlProcessor) transaction.getProcessor( QueryLanguage.CYPHER );

        List<Result> results = new ArrayList<>();

        InformationManager queryAnalyzer = null;
        long executionTime = 0;
        try {
            if ( request.analyze ) {
                transaction.getQueryAnalyzer().setSession( session );
            }

            queryAnalyzer = attachAnalyzerIfSpecified( request, observer, transaction );

            executionTime = System.nanoTime();
            ;

            Statement statement = transaction.createStatement();
            CypherQueryParameters parameters = new CypherQueryParameters( query, NamespaceType.GRAPH, request.database );

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().start( "Parsing" );
            }
            List<? extends Node> statements = cypherProcessor.parse( query );
            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

            int i = 0;
            List<String> splits = List.of( query.split( ";" ) );
            assert statements.size() <= splits.size();
            for ( Node node : statements ) {
                CypherStatement stmt = (CypherStatement) node;

                if ( cypherProcessor.needsDdlGeneration( node, parameters ) ) {
                    cypherProcessor.autoGenerateDDL(
                            Crud.getTransaction( request.analyze, request.cache, transactionManager, userId, databaseId, "HTTP Interface Cypher (auto)" ).createStatement(),
                            node,
                            parameters );
                }

                if ( stmt.isDDL() ) {
                    cypherProcessor.prepareDdl( statement, node, parameters );
                    Result result = new Result( 1 ).setGeneratedQuery( splits.get( i ) ).setXid( transaction.getXid().toString() );
                    results.add( result );
                } else {
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Translation" );
                    }
                    AlgRoot logicalRoot = cypherProcessor.translate( statement, stmt, parameters );
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Translation" );
                    }

                    // Prepare
                    PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Execution" );
                    }
                    results.add( getResult( QueryLanguage.CYPHER, statement, request, query, polyImplementation, transaction, query.toLowerCase().contains( " limit " ) ).setNamespaceName( request.database ) );
                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Execution" );
                    }
                }
                i++;
            }


        } catch ( Throwable t ) {
            printLog( t, request );
            attachError( transaction, results, query, t );
        }

        commitAndFinish( transaction, queryAnalyzer, results, executionTime );

        return results;
    }


    private static void printLog( Throwable t, QueryRequest request ) {
        log.warn( "Failed during execution\nquery:" + request.query + "\nMsg:" + t.getMessage() );
    }


    private static void attachError( Transaction transaction, List<Result> results, String query, Throwable t ) {
        //String msg = t.getMessage() == null ? "" : t.getMessage();
        Result result = new Result( t ).setGeneratedQuery( query ).setXid( transaction.getXid().toString() );

        if ( transaction.isActive() ) {
            try {
                transaction.rollback();
            } catch ( TransactionException e ) {
                throw new RuntimeException( "Error while rolling back the failed transaction." );
            }
        }

        results.add( result );
    }


    @NotNull
    public static Result getResult( QueryLanguage language, Statement statement, QueryRequest request, String query, PolyImplementation result, Transaction transaction, final boolean noLimit ) {
        Catalog catalog = Catalog.getInstance();

        List<List<Object>> rows = result.getRows( statement, noLimit ? -1 : language == QueryLanguage.CYPHER ? RuntimeConfig.UI_NODE_AMOUNT.getInteger() : RuntimeConfig.UI_PAGE_SIZE.getInteger() );

        boolean hasMoreRows = result.hasMoreRows();

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
        for ( AlgDataTypeField metaData : result.rowType.getFieldList() ) {
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
                .setNamespaceType( result.getNamespaceType() )
                .setNamespaceName( request.database )
                .setLanguage( language )
                .setAffectedRows( data.size() )
                .setHasMoreRows( hasMoreRows )
                .setXid( transaction.getXid().toString() )
                .setGeneratedQuery( query );
    }


    /**
     * Creates a new document collection
     */
    public void createCollection( final Context ctx ) {
        EditCollectionRequest request = ctx.bodyAsClass( EditCollectionRequest.class );
        Transaction transaction = crud.getTransaction();

        String query = String.format( "db.createCollection(%s)", request.collection );

        Result result;
        try {
            anyMongoQuery( null, new QueryRequest( query, false, false, "CYPHER", request.database ), crud.getTransactionManager(), crud.getUserId(), crud.getDatabaseId(), null );

            result = new Result( 1 ).setGeneratedQuery( query.toString() );
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while creating a table", e );
            result = new Result( e ).setGeneratedQuery( query );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback createCollection statement: {}", ex.getMessage(), ex );
            }
        }
        ctx.json( result );
    }


    /**
     * This query returns a list of all available document databases (Polypheny schema),
     * as a query result
     */
    public void getDocumentDatabases( final Context ctx ) {
        Map<String, String> names = Catalog.getInstance()
                .getSchemas( Catalog.defaultDatabaseId, null )
                .stream()
                .collect( Collectors.toMap( CatalogSchema::getName, s -> s.namespaceType.name() ) );

        String[][] data = names.entrySet().stream().map( n -> new String[]{ n.getKey(), n.getValue() } ).toArray( String[][]::new );
        ctx.json( new Result( new DbColumn[]{ new DbColumn( "Database/Schema" ), new DbColumn( "Type" ) }, data ) );
    }


    public void getGraphPlacements( final Context ctx ) {
        Index index = ctx.bodyAsClass( Index.class );
        ctx.json( getPlacements( index ) );
    }


    private Placement getPlacements( final Index index ) {
        Catalog catalog = Catalog.getInstance();
        String graphName = index.getSchema();
        List<CatalogGraphDatabase> graphs = catalog.getGraphs( Catalog.defaultDatabaseId, new Pattern( graphName ) );
        if ( graphs.size() != 1 ) {
            log.error( "The requested graph does not exist." );
            return new Placement( new RuntimeException( "The requested graph does not exist." ) );
        }
        CatalogGraphDatabase graph = graphs.get( 0 );
        EntityType type = EntityType.ENTITY;
        Placement p = new Placement( false, List.of(), EntityType.ENTITY );
        if ( type == EntityType.VIEW ) {

            return p;
        } else {
            for ( int adapterId : graph.placements ) {
                CatalogGraphPlacement placement = catalog.getGraphPlacement( graph.id, adapterId );
                Adapter adapter = AdapterManager.getInstance().getAdapter( placement.adapterId );
                p.addAdapter( new Placement.GraphStore(
                        adapter.getUniqueName(),
                        adapter.getAdapterName(),
                        catalog.getGraphPlacements( adapterId ),
                        adapter.getSupportedNamespaceTypes().contains( NamespaceType.GRAPH ) ) );
            }
            return p;
        }

    }


    public void getFixedFields( Context context ) {
        Catalog catalog = Catalog.getInstance();
        UIRequest request = context.bodyAsClass( UIRequest.class );
        Result result;
        List<DbColumn> cols = new ArrayList<>();

        result = new Result( cols.toArray( new DbColumn[0] ), null );
        context.json( result );

    }


    public void getCollectionPlacements( Context context ) {
        Index index = context.bodyAsClass( Index.class );
        String namespace = index.getSchema();
        String collectionName = index.getTable();
        Catalog catalog = Catalog.getInstance();
        long namespaceId;
        try {
            namespaceId = catalog.getSchema( Catalog.defaultDatabaseId, namespace ).id;
        } catch ( UnknownSchemaException e ) {
            context.json( new Placement( e ) );
            return;
        }
        List<CatalogCollection> collections = catalog.getCollections( namespaceId, new Pattern( collectionName ) );

        if ( collections.size() != 1 ) {
            context.json( new Placement( new UnknownCollectionException( 0 ) ) );
            return;
        }

        CatalogCollection collection = catalog.getCollection( collections.get( 0 ).id );

        Placement placement = new Placement( false, List.of(), EntityType.ENTITY );

        for ( Integer adapterId : collection.placements ) {
            Adapter adapter = AdapterManager.getInstance().getAdapter( adapterId );
            List<CatalogCollectionPlacement> placements = catalog.getCollectionPlacementsByAdapter( adapterId );
            placement.addAdapter( new DocumentStore( adapter.getUniqueName(), adapter.getAdapterName(), placements, adapter.getSupportedNamespaceTypes().contains( NamespaceType.DOCUMENT ) ) );
        }

        context.json( placement );
    }

}

