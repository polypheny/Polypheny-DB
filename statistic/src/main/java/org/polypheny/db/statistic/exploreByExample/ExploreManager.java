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

package org.polypheny.db.statistic.exploreByExample;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.statistic.StatisticQueryColumn;
import org.polypheny.db.statistic.StatisticResult;
import org.polypheny.db.statistic.StatisticsManager;


public class ExploreManager {

    private static ExploreManager INSTANCE = null;
    private Map<Integer, Explore> explore = new HashMap<>();
    private final AtomicInteger atomicId = new AtomicInteger();
    private ExploreQueryProcessor exploreQueryProcessor;

    public void setExploreQueryProcessor( ExploreQueryProcessor exploreQueryProcessor ) {
        this.exploreQueryProcessor = exploreQueryProcessor;
    }


    public synchronized static ExploreManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new ExploreManager();
        }
        return INSTANCE;
    }


    public Explore classifyData( Integer id, String[][] classified ) {
        List<String[]> labeled = new ArrayList<>();

        for ( String[] data : classified ) {
            if ( !(data[data.length - 1].equals( "?" )) ) {
                labeled.add( data );
            }
        }

        if ( id != null && explore.containsKey( id ) && explore.get( id ).getDataType() != null ) {
            explore.get( id ).classifyAllData( labeled, getAllData( explore.get( id ).getQuery() ) );
            return explore.get( id );
        } else {
            System.out.println( "Fehler" );
            return null;
        }
    }


    public Explore createSqlQuery( Integer id, String query, List<String> typeInfo ) {

        if ( id == null ) {
            int identifier = atomicId.getAndIncrement();
            explore.put( identifier, new Explore( identifier, query, typeInfo, this.exploreQueryProcessor ) );
            explore.get( identifier ).createSQLStatement();
            return explore.get( identifier );
        }
        return null;
    }


    public Explore exploreData( Integer id, String[][] classified, String[] dataType ) {
        List<String[]> labeled = new ArrayList<>();
        List<String[]> unlabeled = new ArrayList<>();

        for ( String[] data : classified ) {
            if ( !(data[data.length - 1].equals( "?" )) ) {
                labeled.add( data );
                unlabeled.add( data );
            } else if ( data[data.length - 1].equals( "?" ) ) {
                unlabeled.add( data );
            }
        }

        if ( id != null && explore.containsKey( id ) && explore.get( id ).getDataType() != null ) {
            explore.get( id ).updateExploration( labeled );
            return explore.get( id );
        } else if ( id != null && explore.containsKey( id ) && explore.get( id ).getDataType() == null ) {
            explore.get( id ).setLabeled( labeled );
            explore.get( id ).setUnlabeled( unlabeled );
            explore.get( id ).setUniqueValues( getStatistics( explore.get( id ).getQuery() ) );
            explore.get( id ).setDataType( dataType );
            explore.get( id ).exploreUserInput();
            return explore.get( id );
        } else {
            int identifier = atomicId.getAndIncrement();
            explore.get( identifier ).exploreUserInput();
            return explore.get( identifier );
        }
    }


    private List<List<String>> getStatistics( String query ) {

        List<List<String>> uniqueValues = new ArrayList<>();
        StatisticsManager<?> stats = StatisticsManager.getInstance();
        List<StatisticQueryColumn> values = stats.getAllUniqueValues( prepareColInfo( query ), query );

        for ( StatisticQueryColumn uniqueValue : values ) {
            uniqueValues.add( Arrays.asList( uniqueValue.getData() ) );
        }

        List<String> trueFalse = new ArrayList<>();
        trueFalse.add( "true" );
        trueFalse.add( "false" );
        uniqueValues.add( trueFalse );

        return uniqueValues;
    }


    private List<String> prepareColInfo( String query ) {
        return Arrays.asList( query.replace( "SELECT", "" ).split( "\nFROM" )[0].split( "," ) );
    }


    /**
     * TODO Isabel add possiblity to change limit here and hand over to getTable in StatisticsManager
     * Get the whole dataset
     */
    private List<String[]> getAllData( String query ) {

        String queryLimit = query + "LIMIT 5000";

        StatisticsManager<?> stats = StatisticsManager.getInstance();
        StatisticResult statisticResult = stats.getTable( queryLimit );
        StatisticQueryColumn[] columns = statisticResult.getColumns();
        List<String[]> allDataTable = new ArrayList<>();
        int len = 0;
        for ( StatisticQueryColumn column : columns ) {
            allDataTable.add( column.getData() );
            len = column.getData().length;
        }
        String[] questionMark = new String[len];
        for ( int j = 0; j < len; j++ ) {
            questionMark[j] = "?";
        }
        allDataTable.add( questionMark );

        return allDataTable;
    }

}
