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

package org.polypheny.db;

import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;


public abstract class StatisticsManager<T extends Comparable<T>> implements PropertyChangeListener {

    public static StatisticsManager<?> INSTANCE = null;


    public static StatisticsManager<?> setAndGetInstance( StatisticsManager<?> transaction ) {
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


    // Use relNode to update
    public abstract void tablesToUpdate( long tableId );

    // Use cache if possible
    public abstract void tablesToUpdate( long tableId, Map<Long, List<Object>> changedValues, String type, long schemaId );

    protected abstract void reevaluateTable( long tableId );

    public abstract void displayInformation();

    public abstract void asyncReevaluateAllStatistics();

    public abstract void deleteTableToUpdate( long tableId, long schemaId );

    public abstract void updateRowCountPerTable( long tableId, int number, String source );

    public abstract void setIndexSize( long tableId, int indexSize );

    public abstract void setTableCalls( long tableId, String kind );

    public abstract String getRevalId();

    public abstract void setRevalId( String revalId );

    public abstract Map<?, ?> getStatisticSchemaMap();

    public abstract Object getTableStatistic( long schemaId, long tableId );

    public abstract Integer rowCountPerTable( long tableId );

    public abstract void updateCommitRollback( boolean committed );

    public abstract Object getDashboardInformation();

    public abstract void initializeStatisticSettings();

    public abstract void updateColumnName( CatalogColumn catalogColumn, String newName );

    public abstract void updateTableName( CatalogTable catalogTable, String newName );

    public abstract void updateSchemaName( CatalogSchema catalogSchema, String newName );

}
