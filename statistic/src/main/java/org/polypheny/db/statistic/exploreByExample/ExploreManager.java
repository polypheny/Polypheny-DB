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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


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

        explore.get( id ).classifyAllData( labeled );
        return explore.get( id );
    }


    public Explore createSqlQuery( Integer id, String query ) {

        if ( id == null ) {
            int identifier = atomicId.getAndIncrement();
            explore.put( identifier, new Explore( identifier, query, this.exploreQueryProcessor ) );
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

        if ( id != null && explore.containsKey( id ) && explore.get( id ).getLabeled() != null ) {
            explore.get( id ).updateExploration( labeled );
        } else {
            explore.get( id ).setLabeled( labeled );
            explore.get( id ).setUnlabeled( unlabeled );
            explore.get( id ).setUniqueValues( explore.get( id ).getStatistics( explore.get( id ).getQuery() ) );
            explore.get( id ).setDataType( dataType );
            explore.get( id ).exploreUserInput();
        }
        return explore.get( id );
    }

}
