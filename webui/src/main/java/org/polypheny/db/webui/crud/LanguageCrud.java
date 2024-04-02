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

package org.polypheny.db.webui.crud;

import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import io.javalin.http.Context;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.websocket.api.Session;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.TemporalFileManager;
import org.polypheny.db.webui.models.IndexModel;
import org.polypheny.db.webui.models.PlacementModel;
import org.polypheny.db.webui.models.PlacementModel.DocumentStore;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.catalog.FieldDefinition;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition;
import org.polypheny.db.webui.models.catalog.UiColumnDefinition.UiColumnDefinitionBuilder;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.DocResult;
import org.polypheny.db.webui.models.results.GraphResult;
import org.polypheny.db.webui.models.results.GraphResult.GraphResultBuilder;
import org.polypheny.db.webui.models.results.QueryType;
import org.polypheny.db.webui.models.results.RelationalResult;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.webui.models.results.Result.ResultBuilder;

@Getter
@Slf4j
public class LanguageCrud {


    public static Crud crud;

    protected final static Map<QueryLanguage, TriFunction<ExecutedContext, UIRequest, Statement, ResultBuilder<?, ?, ?, ?>>> REGISTER = new HashMap<>();


    public LanguageCrud( Crud crud ) {
        LanguageCrud.crud = crud;
    }


    public static void addToResult( QueryLanguage queryLanguage, TriFunction<ExecutedContext, UIRequest, Statement, ResultBuilder<?, ?, ?, ?>> toResult ) {
        REGISTER.put( queryLanguage, toResult );
    }


    public static TriFunction<ExecutedContext, UIRequest, Statement, ResultBuilder<?, ?, ?, ?>> getToResult( QueryLanguage queryLanguage ) {
        return REGISTER.get( queryLanguage );
    }


    public static void deleteToResult( QueryLanguage language ) {
        REGISTER.remove( language );
    }


    public static void anyQuery( Context ctx ) {
        QueryRequest request = ctx.bodyAsClass( QueryRequest.class );
        QueryLanguage language = QueryLanguage.from( request.language );

        QueryContext context = QueryContext.builder()
                .query( request.query )
                .language( language )
                .isAnalysed( request.analyze )
                .usesCache( request.cache )
                .origin( "Polypheny-UI" )
                .namespaceId( getNamespaceIdOrDefault( request.namespace ) )
                .batch( request.noLimit ? -1 : crud.getPageSize() )
                .transactionManager( crud.getTransactionManager() ).build();
        ctx.json( anyQueryResult( context, request ) );
    }


    public static long getNamespaceIdOrDefault( String namespace ) {
        return namespace == null ? Catalog.defaultNamespaceId : Catalog.snapshot().getNamespace( namespace ).orElseThrow().id;
    }


    public static List<? extends Result<?, ?>> anyQueryResult( QueryContext context, UIRequest request ) {
        context = context.getLanguage().limitRemover().apply( context );
        Transaction transaction = !context.getTransactions().isEmpty() ? context.getTransactions().get( 0 ) : context.getTransactionManager().startTransaction( context.getUserId(), Catalog.defaultNamespaceId, context.isAnalysed(), context.getOrigin() );
        transaction.setUseCache( context.isUsesCache() );
        attachAnalyzerIfSpecified( context, crud, transaction );

        List<ExecutedContext> executedContexts = LanguageManager.getINSTANCE().anyQuery( context.addTransaction( transaction ) );

        List<Result<?, ?>> results = new ArrayList<>();
        TriFunction<ExecutedContext, UIRequest, Statement, ResultBuilder<?, ?, ?, ?>> builder = REGISTER.get( context.getLanguage() );

        for ( ExecutedContext executedContext : executedContexts ) {
            if ( executedContext.getException().isPresent() ) {
                log.warn( "Caught exception", executedContext.getException().get() );
                return List.of( buildErrorResult( transaction, executedContext, executedContext.getException().get() ).build() );
            }

            results.add( builder.apply( executedContext, request, executedContext.getStatement() ).build() );
        }

        commitAndFinish( executedContexts, transaction.getQueryAnalyzer(), results, executedContexts.stream().map( ExecutedContext::getExecutionTime ).reduce( Long::sum ).orElse( -1L ) );

        return results;
    }


    public static void commitAndFinish( List<ExecutedContext> executedContexts, InformationManager queryAnalyzer, List<Result<?, ?>> results, long executionTime ) {
        executionTime = System.nanoTime() - executionTime;
        String commitStatus = "Error on starting committing";
        for ( Transaction transaction : executedContexts.stream().flatMap( c -> c.getQuery().getTransactions().stream() ).toList() ) {
            // this has a lot of unnecessary no-op commits atm
            try {
                transaction.commit();
                commitStatus = "Committed";
            } catch ( TransactionException e ) {
                log.error( "Caught exception", e );
                results.add( RelationalResult.builder().error( e.getMessage() ).build() );
                try {
                    transaction.rollback();
                    commitStatus = "Rolled back";
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", e );
                    commitStatus = "Error while rolling back";
                }
            }
        }

        if ( queryAnalyzer != null ) {
            Crud.attachQueryAnalyzer( queryAnalyzer, executionTime, commitStatus, results.size() );
        }
    }


    @Nullable
    public static InformationManager attachAnalyzerIfSpecified( QueryContext context, InformationObserver observer, Transaction transaction ) {
        // This is not a nice solution. In case of a sql script with auto commit only the first statement is analyzed
        // and in case of auto commit of, the information is overwritten
        InformationManager queryAnalyzer = null;
        if ( context.isAnalysed() ) {
            queryAnalyzer = transaction.getQueryAnalyzer().observe( observer );
        }
        return queryAnalyzer;
    }


    public static PolyGraph getGraph( String namespace, TransactionManager manager, Session session ) {
        QueryLanguage language = QueryLanguage.from( "cypher" );
        Transaction transaction = Crud.getTransaction( false, false, manager, Catalog.defaultUserId, Catalog.defaultNamespaceId, "getGraph" );
        ImplementationContext context = LanguageManager.getINSTANCE().anyPrepareQuery(
                QueryContext.builder()
                        .query( "MATCH (*) RETURN *" )
                        .language( language )
                        .origin( transaction.getOrigin() )
                        .namespaceId( getNamespaceIdOrDefault( namespace ) )
                        .transactionManager( manager )
                        .informationTarget( i -> i.setSession( session ) )
                        .build(), transaction ).get( 0 );

        if ( context.getException().isPresent() ) {
            return new PolyGraph( PolyMap.of( new HashMap<>() ), PolyMap.of( new HashMap<>() ) );
        }

        ResultIterator iterator = context.execute( context.getStatement() ).getIterator();
        List<List<PolyValue>> res = iterator.getNextBatch();

        try {
            iterator.close();
            transaction.commit();
        } catch ( Exception | TransactionException e ) {
            throw new GenericRuntimeException( "Error while committing graph retrieval query." );
        }

        if ( res.size() == 1 && res.get( 0 ).size() == 1 && res.get( 0 ).get( 0 ).isGraph() ) {
            return res.get( 0 ).get( 0 ).asGraph();
        }

        throw new GenericRuntimeException( "Error while retrieving graph." );
    }


    public static ResultBuilder<?, ?, ?, ?> buildErrorResult( Transaction transaction, ExecutedContext context, Throwable t ) {
        //String msg = t.getMessage() == null ? "" : t.getMessage();
        ResultBuilder<?, ?, ?, ?> result = switch ( context.getQuery().getLanguage().dataModel() ) {
            case RELATIONAL -> RelationalResult.builder().error( t == null ? null : t.getMessage() ).exception( t ).query( context.getQuery().getQuery() ).xid( transaction.getXid().toString() );
            case DOCUMENT -> DocResult.builder().error( t == null ? null : t.getMessage() ).exception( t ).query( context.getQuery().getQuery() ).xid( transaction.getXid().toString() );
            case GRAPH -> GraphResult.builder().error( t == null ? null : t.getMessage() ).exception( t ).query( context.getQuery().getQuery() ).xid( transaction.getXid().toString() );
        };

        if ( transaction.isActive() ) {
            try {
                transaction.rollback();
            } catch ( TransactionException e ) {
                throw new GenericRuntimeException( "Error while rolling back the failed transaction." );
            }
        }

        return result;
    }


    @NotNull
    public static ResultBuilder<?, ?, ?, ?> getRelResult( ExecutedContext context, UIRequest request, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        ResultIterator iterator = context.getIterator();
        List<List<PolyValue>> rows = new ArrayList<>();
        try {
            for ( int i = 0; i < request.currentPage; i++ ) {
                rows = iterator.getNextBatch();
            }

            iterator.close();
        } catch ( Exception e ) {
            return buildErrorResult( statement.getTransaction(), context, e );
        }

        boolean hasMoreRows = context.getIterator().hasMoreRows();

        LogicalTable table = null;
        if ( request.entityId != null ) {
            table = Catalog.snapshot().rel().getTable( request.entityId ).orElseThrow();
        }

        List<UiColumnDefinition> header = new ArrayList<>();
        for ( AlgDataTypeField field : context.getIterator().getImplementation().tupleType.getFields() ) {
            String columnName = field.getName();

            String filter = getFilter( field, request.filter );

            SortState sort = getSortState( field, request.sortState );

            UiColumnDefinitionBuilder<?, ?> dbCol = UiColumnDefinition.builder()
                    .name( field.getName() )
                    .dataType( field.getType().getFullTypeString() )
                    .nullable( field.getType().isNullable() == (ResultSetMetaData.columnNullable == 1) )
                    .precision( field.getType().getPrecision() )
                    .sort( sort )
                    .filter( filter );

            // Get column default values
            if ( table != null ) {
                Optional<LogicalColumn> optional = catalog.getSnapshot().rel().getColumn( table.id, columnName );
                if ( optional.isPresent() ) {
                    if ( optional.get().defaultValue != null ) {
                        dbCol.defaultValue( optional.get().defaultValue.value.toJson() );
                    }
                }
            }
            header.add( dbCol.build() );
        }

        List<String[]> data = computeResultData( rows, header, statement.getTransaction() );

        return RelationalResult
                .builder()
                .header( header.toArray( new UiColumnDefinition[0] ) )
                .data( data.toArray( new String[0][] ) )
                .dataModel( context.getIterator().getImplementation().getDataModel() )
                .namespace( request.namespace )
                .language( context.getQuery().getLanguage() )
                .affectedTuples( data.size() )
                .queryType( QueryType.from( context.getImplementation().getKind() ) )
                .hasMore( hasMoreRows )
                .xid( statement.getTransaction().getXid().toString() )
                .query( context.getQuery().getQuery() );
    }


    public static List<String[]> computeResultData( final List<List<PolyValue>> rows, final List<UiColumnDefinition> header, final Transaction transaction ) {
        List<String[]> data = new ArrayList<>();
        for ( List<PolyValue> row : rows ) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for ( PolyValue o : row ) {
                if ( o == null || o.isNull() ) {
                    temp[counter] = null;
                } else {
                    String columnName = String.valueOf( header.get( counter ).name.hashCode() );
                    File mmFolder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "/tmp" );
                    mmFolder.mkdirs();
                    ContentInfoUtil util = new ContentInfoUtil();
                    if ( List.of( PolyType.FILE.getName(), PolyType.VIDEO.getName(), PolyType.AUDIO.getName(), PolyType.IMAGE.getName() ).contains( header.get( counter ).dataType ) ) {
                        ContentInfo info = util.findMatch( o.asBlob().value );
                        String extension = "";
                        if ( info != null && info.getFileExtensions() != null && info.getFileExtensions().length > 0 ) {
                            extension = "." + info.getFileExtensions()[0];
                        }
                        File f = new File( mmFolder, columnName + "_" + UUID.randomUUID() + extension );
                        try ( FileOutputStream fos = new FileOutputStream( f ) ) {
                            fos.write( o.asBlob().value );
                        } catch ( IOException e ) {
                            throw new GenericRuntimeException( "Could not place file in mm folder", e );
                        }
                        temp[counter] = f.getName();
                        TemporalFileManager.addFile( transaction.getXid().toString(), f );
                    } else {
                        temp[counter] = o.toJson();
                    }
                }
                counter++;
            }
            data.add( temp );
        }
        return data;
    }


    public static ResultBuilder<?, ?, ?, ?> getGraphResult( ExecutedContext context, UIRequest request, Statement statement ) {
        ResultIterator iterator = context.getIterator();
        List<PolyValue[]> data;
        try {
            data = iterator.getArrayRows();

            iterator.close();

            GraphResultBuilder<?, ?> builder = GraphResult.builder()
                    .data( data.stream().map( r -> Arrays.stream( r ).map( LanguageCrud::toJson ).toArray( String[]::new ) ).toArray( String[][]::new ) )
                    .header( context.getIterator().getImplementation().tupleType.getFields().stream().map( FieldDefinition::of ).toArray( FieldDefinition[]::new ) )
                    .query( context.getQuery().getQuery() )
                    .language( context.getQuery().getLanguage() )
                    .queryType( QueryType.from( context.getImplementation().getKind() ) )
                    .dataModel( context.getIterator().getImplementation().getDataModel() )
                    .affectedTuples( data.size() )
                    .xid( statement.getTransaction().getXid().toString() )
                    .namespace( request.namespace );

            if ( Kind.DML.contains( context.getIterator().getImplementation().getKind() ) ) {
                builder.affectedTuples( data.get( 0 )[0].asNumber().longValue() );
            }
            return builder;

        } catch ( Exception e ) {
            return buildErrorResult( statement.getTransaction(), context, e );
        }
    }


    public static ResultBuilder<?, ?, ?, ?> getDocResult( ExecutedContext context, UIRequest request, Statement statement ) {
        ResultIterator iterator = context.getIterator();
        List<List<PolyValue>> data = new ArrayList<>();

        try {
            for ( int i = 0; i < request.currentPage; i++ ) {
                data = iterator.getNextBatch();
            }

            iterator.close();

            boolean hasMoreRows = context.getIterator().hasMoreRows();

            return DocResult.builder()
                    .header( new FieldDefinition[]{ FieldDefinition.builder().name( "Document" ).dataType( DataModel.DOCUMENT.name() ).build() } )
                    .data( data.stream().map( d -> d.get( 0 ).toJson() ).toArray( String[]::new ) )
                    .query( context.getQuery().getQuery() )
                    .language( context.getQuery().getLanguage() )
                    .queryType( QueryType.from( context.getImplementation().getKind() ) )
                    .hasMore( hasMoreRows )
                    .affectedTuples( data.size() )
                    .xid( statement.getTransaction().getXid().toString() )
                    .dataModel( context.getIterator().getImplementation().getDataModel() )
                    .namespace( request.namespace );
        } catch ( Exception e ) {
            return buildErrorResult( statement.getTransaction(), context, e );
        }
    }


    private static String toJson( @Nullable PolyValue src ) {
        return src == null
                ? null
                : Catalog.mode == RunMode.TEST ? src.toTypedJson() : src.toJson();
    }


    private static String getFilter( AlgDataTypeField field, Map<String, String> filter ) {
        if ( filter != null && filter.containsKey( field.getName() ) ) {
            return filter.get( field.getName() );
        }
        return "";
    }


    private static SortState getSortState( AlgDataTypeField field, Map<String, SortState> sortState ) {
        if ( sortState != null && sortState.containsKey( field.getName() ) ) {
            return sortState.get( field.getName() );
        }
        return new SortState();
    }


    /**
     * This query returns a list of all available document databases (Polypheny schema),
     * as a query result
     */
    public void getDocumentDatabases( final Context ctx ) {
        Map<String, String> names = Catalog.getInstance().getSnapshot()
                .getNamespaces( null )
                .stream()
                .collect( Collectors.toMap( LogicalNamespace::getName, s -> s.dataModel.name() ) );

        String[][] data = names.entrySet().stream().map( n -> new String[]{ n.getKey(), n.getValue() } ).toArray( String[][]::new );
        ctx.json( RelationalResult
                .builder()
                .header( new UiColumnDefinition[]{ UiColumnDefinition.builder().name( "Database/Schema" ).build(), UiColumnDefinition.builder().name( "Type" ).build() } )
                .data( data )
                .build() );
    }


    public void getGraphPlacements( final Context ctx ) {
        IndexModel index = ctx.bodyAsClass( IndexModel.class );
        ctx.json( getPlacements( index ) );
    }


    private PlacementModel getPlacements( final IndexModel index ) {
        Catalog catalog = Catalog.getInstance();
        LogicalGraph graph = Catalog.snapshot().graph().getGraph( index.namespaceId ).orElseThrow();
        EntityType type = EntityType.ENTITY;
        PlacementModel p = new PlacementModel( false, List.of(), EntityType.ENTITY );
        if ( type == EntityType.VIEW ) {
            return p;
        } else {
            List<AllocationPlacement> placements = catalog.getSnapshot().alloc().getPlacementsFromLogical( graph.id );
            for ( AllocationPlacement placement : placements ) {
                Adapter<?> adapter = AdapterManager.getInstance().getAdapter( placement.adapterId ).orElseThrow();
                p.addAdapter( new PlacementModel.GraphStore(
                        adapter.getUniqueName(),
                        adapter.getUniqueName(),
                        catalog.getSnapshot().alloc().getFromLogical( placement.adapterId ),
                        false ) );
            }
            return p;
        }

    }


    public void getFixedFields( Context context ) {
        Catalog catalog = Catalog.getInstance();
        UIRequest request = context.bodyAsClass( UIRequest.class );
        RelationalResult result;
        List<UiColumnDefinition> cols = new ArrayList<>();

        result = RelationalResult.builder().header( cols.toArray( new UiColumnDefinition[0] ) ).data( null ).build();
        context.json( result );

    }


    public void getCollectionPlacements( Context context ) {
        IndexModel index = context.bodyAsClass( IndexModel.class );
        Catalog catalog = Catalog.getInstance();
        LogicalCollection collection = catalog.getSnapshot().doc().getCollection( index.entityId ).orElseThrow();

        PlacementModel p = new PlacementModel( false, List.of(), EntityType.ENTITY );

        List<AllocationEntity> allocs = catalog.getSnapshot().alloc().getFromLogical( collection.id );

        for ( AllocationEntity allocation : allocs ) {
            Adapter<?> adapter = AdapterManager.getInstance().getAdapter( allocation.adapterId ).orElseThrow();
            p.addAdapter( new DocumentStore(
                    adapter.getUniqueName(),
                    adapter.getUniqueName(),
                    catalog.getSnapshot().alloc().getEntitiesOnAdapter( allocation.adapterId )
                            .orElse( List.of() ),
                    false ) );
        }

        context.json( p );
    }


    @FunctionalInterface
    public interface TriFunction<First, Second, Third, Result> {

        Result apply( First first, Second second, Third third );

    }

}

