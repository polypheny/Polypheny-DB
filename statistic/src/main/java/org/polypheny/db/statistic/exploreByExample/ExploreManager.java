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
    private Map<Integer, ExploreProcess> processes = new HashMap<>(  );
    private Map<Integer, Explore> explore = new HashMap<>(  );
    private final AtomicInteger atomicId = new AtomicInteger(  );


    public synchronized static ExploreManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new ExploreManager();
        }
        return INSTANCE;
    }

/*
    public String processUpdate(Integer id, String[][] data, String[] columnInfo, String query, String[] dataTyp ) throws Exception {
        if ( id != null && processes.containsKey( id ) ) {
            processes.get( id ).updateClassification(data, columnInfo, query, dataTyp);
        } else {
            int identifier = atomicId.getAndIncrement();
            processes.put( identifier, new ExploreProcess(identifier, data, columnInfo, query, dataTyp) );
            return processes.get( identifier ).prepareUserInput();
        }

        return "test";
    }

 */


    public Explore exploreData( Integer id, String[] columnInfo, String query, String[][] labeled, String[][] unlabeled, String[] dataType ) {

        if ( id != null && explore.containsKey( id ) ) {
            System.out.println( "inside if" );
            explore.get( id ).updateExploration(labeled);
            return explore.get( id );
        } else {
            int identifier = atomicId.getAndIncrement();
            explore.put( identifier, new Explore(identifier, getStatistics(columnInfo, query), labeled, unlabeled, dataType) );
            explore.get( identifier ).exploreUserInput();
            return explore.get( identifier );
        }

    }

    private List<List<String>> getStatistics(String[] columnInfo, String query) {
        List<List<String>> uniqueValues = new ArrayList<>();
        StatisticsManager<?> stats = StatisticsManager.getInstance();

        List<StatisticQueryColumn> values = stats.getAllUniqueValues( Arrays.asList( columnInfo ), query );

        for ( StatisticQueryColumn uniqueValue : values ) {
            uniqueValues.add( Arrays.asList( uniqueValue.getData() ) );
        }

        List<String> trueFalse = new ArrayList<>();
        trueFalse.add( "true" );
        trueFalse.add( "false" );
        uniqueValues.add( trueFalse );


        return uniqueValues;

    }

}
