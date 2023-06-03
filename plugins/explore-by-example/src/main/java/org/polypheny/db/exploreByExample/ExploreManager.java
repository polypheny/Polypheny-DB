/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.exploreByExample;


import io.javalin.http.Context;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.exploreByExample.requests.ClassifyAllData;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.Crud.QueryExecutionException;
import org.polypheny.db.webui.models.requests.ExploreData;
import org.polypheny.db.webui.models.requests.ExploreTables;
import org.polypheny.db.webui.models.requests.QueryExplorationRequest;
import org.polypheny.db.webui.models.results.ExploreResult;
import org.polypheny.db.webui.models.results.Result;
import org.polypheny.db.webui.models.results.Result.ResultBuilder;

@Slf4j
public class ExploreManager {

    private static ExploreManager INSTANCE = null;
    private final Map<Integer, Explore> explore = new HashMap<>();
    private final AtomicInteger atomicId = new AtomicInteger();
    private ExploreQueryProcessor exploreQueryProcessor;


    public synchronized static ExploreManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new ExploreManager();
        }
        return INSTANCE;
    }


    public void setExploreQueryProcessor( ExploreQueryProcessor exploreQueryProcessor ) {
        this.exploreQueryProcessor = exploreQueryProcessor;
    }


    public Explore getExploreInformation( Integer id ) {
        return explore.get( id );
    }


    /**
     * Creates the initial sql statement for initial query.
     *
     * @param id ExploreID to identify the explore object at this point always null
     * @param query Initial sql query form user interface
     * @return Explore object
     */
    public Explore createSqlQuery( Integer id, String query ) {
        if ( id == null ) {
            int identifier = atomicId.getAndIncrement();
            explore.put( identifier, new Explore( identifier, query, this.exploreQueryProcessor ) );

            if ( explore.get( identifier ).getDataType() == null ) {
                return explore.get( identifier );
            }

            explore.get( identifier ).createSQLStatement();
            return explore.get( identifier );
        }
        return null;
    }


    /**
     * Starts the exploration process or continuous the process, depending on the explore id.
     *
     * @param id Explore ID to identify the explore object
     * @param classified data form user
     * @param dataType for all columns
     * @return Explore Object
     */
    public Explore exploreData( Integer id, String[][] classified, String[] dataType ) {
        List<String[]> labeled = new ArrayList<>();

        for ( String[] data : classified ) {
            if ( !(data[data.length - 1].equals( "?" )) ) {
                labeled.add( data );
            }
        }

        if ( id != null && explore.containsKey( id ) && explore.get( id ).getLabeled() != null ) {
            explore.get( id ).updateExploration( labeled );
        } else {
            explore.get( id ).setLabeled( labeled );
            explore.get( id ).setUniqueValues( explore.get( id ).getStatistics( explore.get( id ).getQuery() ) );
            explore.get( id ).setDataType( dataType );
            explore.get( id ).exploreUserInput();
        }
        return explore.get( id );
    }


    /**
     * Classify all data for final result
     *
     * @param id Explore ID to identify the explore object
     * @param classified data form user
     * @param returnsSql to check if WekaToSQL is active or not
     * @return Explore object
     */
    public Explore classifyData( Integer id, String[][] classified, boolean returnsSql ) {
        List<String[]> labeled = new ArrayList<>();
        for ( String[] data : classified ) {
            if ( !(data[data.length - 1].equals( "?" )) ) {
                labeled.add( data );
            }
        }
        explore.get( id ).classifyAllData( labeled, returnsSql );
        return explore.get( id );
    }


    /**
     * Gets the classified Data from User
     * return possibly interesting Data to User
     */
    public void classifyData( final Context ctx, Crud crud ) {
        ClassifyAllData classifyAllData = ctx.bodyAsClass( ClassifyAllData.class );

        boolean isConvertedToSql = isClassificationToSql();

        Explore explore = classifyData( classifyAllData.id, classifyAllData.classified, isConvertedToSql );

        if ( isConvertedToSql ) {
            Transaction transaction = crud.getTransaction();
            Statement statement = transaction.createStatement();
            ResultBuilder<?, ?> result;

            try {
                result = Crud
                        .executeSqlSelect( statement, classifyAllData, explore.getClassifiedSqlStatement(), false, crud )
                        .generatedQuery( explore.getClassifiedSqlStatement() );
                transaction.commit();
            } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
                log.error( "Caught exception while executing a query from the console", e );
                result = Result.builder()
                        .error( e.getMessage() )
                        .generatedQuery( explore.getClassifiedSqlStatement() );
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rollback", ex );
                }
            }

            result.explorerId( explore.getId() )
                    .currentPage( classifyAllData.cPage )
                    .table( classifyAllData.tableId )
                    .highestPage( (int) Math.ceil( (double) explore.getTableSize() / crud.getPageSize() ) )
                    .classificationInfo( "NoClassificationPossible" )
                    .isConvertedToSql( isConvertedToSql );

            ctx.json( result );
        } else {
            ResultBuilder<?, ?> result = Result.builder()
                    .header( classifyAllData.header )
                    .data( Arrays.copyOfRange( explore.getData(), 0, 10 ) ).classificationInfo( "NoClassificationPossible" )
                    .explorerId( explore.getId() )
                    .currentPage( classifyAllData.cPage ).table( classifyAllData.tableId )
                    .highestPage( (int) Math.ceil( (double) explore.getData().length / crud.getPageSize() ) )
                    .isConvertedToSql( isConvertedToSql );
            ctx.json( result );
        }

    }


    public boolean isClassificationToSql() {
        return RuntimeConfig.EXPLORE_BY_EXAMPLE_TO_SQL.getBoolean();
    }


    /**
     * For pagination within the Explore-by-Example table
     */
    public void getExploreTables( final Context ctx, Crud crud ) {
        ExploreTables exploreTables = ctx.bodyAsClass( ExploreTables.class );
        Transaction transaction = crud.getTransaction();
        Statement statement = transaction.createStatement();

        ResultBuilder<?, ?> result;
        Explore explore = getExploreInformation( exploreTables.id );
        String[][] paginationData;

        String query = explore.getSqlStatement() + " OFFSET " + ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize());

        if ( !explore.isConvertedToSql() && !explore.isClassificationPossible() ) {
            int tablesize = explore.getData().length;

            if ( tablesize >= ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()) && tablesize < ((Math.max( 0, exploreTables.cPage )) * crud.getPageSize()) ) {
                paginationData = Arrays.copyOfRange( explore.getData(), ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()), tablesize );
            } else {
                paginationData = Arrays.copyOfRange( explore.getData(), ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()), ((Math.max( 0, exploreTables.cPage )) * crud.getPageSize()) );
            }
            result = Result.builder().header( exploreTables.columns )
                    .classifiedData( paginationData )
                    .classificationInfo( "NoClassificationPossible" )
                    .explorerId( explore.getId() )
                    .currentPage( exploreTables.cPage )
                    .table( exploreTables.tableId )
                    .highestPage( (int) Math.ceil( (double) tablesize / crud.getPageSize() ) );

            ctx.json( result );
        }

        try {
            result = crud.executeSqlSelect( statement, exploreTables, query );
        } catch ( QueryExecutionException e ) {
            log.error( "Caught exception while fetching a table", e );
            result = Result.builder().error( "Could not fetch table " + exploreTables.tableId );
            try {
                transaction.rollback();
                ctx.status( 500 ).json( result );
            } catch ( TransactionException ex ) {
                log.error( "Could not rollback", ex );
            }
        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while committing transaction", e );
        }
        result.explorerId( explore.getId() )
                .currentPage( exploreTables.cPage )
                .table( exploreTables.tableId );
        int tableSize = explore.getTableSize();

        result.highestPage( (int) Math.ceil( (double) tableSize / crud.getPageSize() ) );

        if ( !explore.isClassificationPossible() ) {
            result.classificationInfo( "NoClassificationPossible" );
        } else {
            result.classificationInfo( "ClassificationPossible" );
        }
        result.includesClassificationInfo( explore.isDataAfterClassification );

        if ( explore.isDataAfterClassification ) {
            int tablesize = explore.getDataAfterClassification().size();
            List<String[]> paginationDataList;
            if ( tablesize >= ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()) && tablesize < ((Math.max( 0, exploreTables.cPage )) * crud.getPageSize()) ) {
                paginationDataList = explore.getDataAfterClassification().subList( ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()), tablesize );
            } else {
                paginationDataList = explore.getDataAfterClassification().subList( ((Math.max( 0, exploreTables.cPage - 1 )) * crud.getPageSize()), ((Math.max( 0, exploreTables.cPage )) * crud.getPageSize()) );
            }

            paginationData = new String[paginationDataList.size()][];
            for ( int i = 0; i < paginationDataList.size(); i++ ) {
                paginationData[i] = paginationDataList.get( i );
            }

            result.classifiedData( paginationData );
        }
        ctx.json( result );

    }


    /**
     * Creates the initial query for the Explore-by-Example process
     */
    public void createInitialExploreQuery( final Context ctx, final Crud crud ) {
        QueryExplorationRequest queryExplorationRequest = ctx.bodyAsClass( QueryExplorationRequest.class );

        ResultBuilder<?, ?> result;

        Explore explore = createSqlQuery( null, queryExplorationRequest.query );
        if ( explore.getDataType() == null ) {
            ctx.status( 400 ).json( Result.builder().error( "Explore by Example is only available for tables with the following datatypes: VARCHAR, INTEGER, SMALLINT, TINYINT, BIGINT, DECIMAL" ) );
            return;
        }

        Transaction transaction = Crud.getTransaction( queryExplorationRequest.analyze, true, crud.getTransactionManager(), crud.getUserId(), crud.getDatabaseId(), "Explore-by-Example" );
        Statement statement = transaction.createStatement();
        try {
            String query = explore.getSqlStatement();
            result = Crud.executeSqlSelect( statement, queryExplorationRequest, query, false, crud ).generatedQuery( query );
            transaction.commit();
        } catch ( QueryExecutionException | TransactionException | RuntimeException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            result = Result.builder().error( e.getMessage() );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", ex );
            }
        }

        result.explorerId( explore.getId() );
        if ( !explore.isClassificationPossible() ) {
            result.classificationInfo( "NoClassificationPossible" );

        } else {
            result.classificationInfo( "ClassificationPossible" );
        }
        result.currentPage( queryExplorationRequest.cPage )
                .table( queryExplorationRequest.tableId )
                .highestPage( (int) Math.ceil( (double) explore.getTableSize() / crud.getPageSize() ) );

        ctx.json( result );
    }


    /**
     * Start Classification, classifies the initial dataset, to show what would be within the final result set
     */
    public void exploration( final Context ctx, final Crud crud ) {
        ExploreData exploreData = ctx.bodyAsClass( ExploreData.class );

        String[] dataType = new String[exploreData.header.length + 1];
        for ( int i = 0; i < exploreData.header.length; i++ ) {
            dataType[i] = exploreData.header[i].dataType;
        }
        dataType[exploreData.header.length] = "VARCHAR";

        Explore explore = exploreData( exploreData.id, exploreData.classified, dataType );

        ctx.json( new ExploreResult( exploreData.header, explore.getDataAfterClassification(), explore.getId(), explore.getBuildGraph() ) );
    }

}
