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

package org.polypheny.db;

import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class StatisticsManager<T extends Comparable<T>> implements PropertyChangeListener {

    public static StatisticsManager<?> INSTANCE = null;

    public static StatisticsManager<?> setAndGetInstance( StatisticsManager transaction ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Overwriting the MaterializedViewManager is not permitted." );
        }
        INSTANCE = transaction;
        return INSTANCE;
    }


    public static StatisticsManager<?> getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "MaterializedViewManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }



    //use relNode to update
    protected abstract void tablesToUpdate( Long tableId );

    //use cache if possible
    public abstract void tablesToUpdate( Long tableId, HashMap<Long, List<Object>> changedValues, String type );

    protected abstract void reevaluateTable( Long tableId );

    public abstract void displayInformation();

    public abstract void asyncReevaluateAllStatistics();

    public abstract void deleteTableToUpdate( Long tableId );

    public abstract void updateRowCountPerTable( Long tableId, int number, boolean isAdding );

    public abstract void setIndexSize( Long tableId, int indexSize );

    public abstract void setTableCalls( Long tableId, String kind );

    public abstract String getRevalId();

    public abstract void setRevalId( String revalId );

    public abstract ConcurrentHashMap<?,?> getStatisticSchemaMap();

    public abstract Object getTableStatistic(Long schemaId, Long tableId);

    public abstract int getNumberOfCommits();

    public abstract int getNumberOfRollbacks();

    public abstract void setNumberOfCommits( int numberOfCommits );

    public abstract void setNumberOfRollbacks(int numberOfRollbacks);

    public abstract Integer rowCountPerTable( Long tableId );

    public abstract void setRowCount( Long tableId, int rowCount );

}
