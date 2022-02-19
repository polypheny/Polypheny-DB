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

package org.polypheny.db.monitoring.statistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringDataPoint.DataPointType;
import org.polypheny.db.monitoring.events.metrics.DdlDataPoint;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.monitoring.repository.MonitoringRepository;


@Slf4j
public class StatisticRepository implements MonitoringRepository {

    /**
     * This method uses monitoring information to update the statistics.
     *
     * @param dataPoint to be processed
     */
    @Override
    public void dataPoint( MonitoringDataPoint dataPoint ) {
        StatisticsManager<?> statisticsManager = StatisticsManager.getInstance();
        statisticsManager.updateCommitRollback( dataPoint.isCommitted() );

        if ( dataPoint.getDataPointType() == DataPointType.DML ) {
            updateDmlStatistics( (DmlDataPoint) dataPoint, statisticsManager );
        } else if ( dataPoint.getDataPointType() == DataPointType.QueryDataPointImpl ) {
            updateQueryStatistics( (QueryDataPointImpl) dataPoint, statisticsManager );
        } else if ( dataPoint.getDataPointType() == DataPointType.DDL ) {
            updateDdlStatistics( (DdlDataPoint) dataPoint, statisticsManager );
        }
    }


    private void updateDdlStatistics( DdlDataPoint dataPoint, StatisticsManager<?> statisticsManager ) {
        if ( dataPoint.getMonitoringType().equals( "TRUNCATE" ) ) {
            statisticsManager.updateRowCountPerTable(
                    dataPoint.getTableId(),
                    0,
                    dataPoint.getMonitoringType() );
            statisticsManager.tablesToUpdate(
                    dataPoint.getTableId(),
                    null,
                    dataPoint.getMonitoringType(),
                    dataPoint.getSchemaId() );
        }
        if ( dataPoint.getMonitoringType().equals( "DROP_TABLE" ) ) {
            statisticsManager.deleteTableToUpdate( dataPoint.getTableId(), dataPoint.getSchemaId() );
        }
        if ( dataPoint.getMonitoringType().equals( "DROP_COLUMN" ) ) {
            statisticsManager.tablesToUpdate(
                    dataPoint.getTableId(),
                    Collections.singletonMap( dataPoint.getColumnId(), null ),
                    dataPoint.getMonitoringType(),
                    dataPoint.getSchemaId() );
        }
    }


    private void updateQueryStatistics( QueryDataPointImpl dataPoint, StatisticsManager<?> statisticsManager ) {
        if ( !dataPoint.getAvailableColumnsWithTable().isEmpty() ) {
            Set<Long> values = new HashSet<>( dataPoint.getAvailableColumnsWithTable().values() );
            boolean isOneTable = values.size() == 1;
            Catalog catalog = Catalog.getInstance();

            if ( isOneTable ) {
                long tableId = values.stream().findFirst().get();
                if ( catalog.checkIfExistsTable( tableId ) ) {
                    statisticsManager.setTableCalls( tableId, dataPoint.getMonitoringType() );

                    // RowCount from UI is only used if there is no other possibility
                    if ( statisticsManager.rowCountPerTable( tableId ) == null || statisticsManager.rowCountPerTable( tableId ) == 0 ) {
                        statisticsManager.updateRowCountPerTable( tableId, dataPoint.getRowCount(), "SET-ROW-COUNT" );
                    }

                    if ( dataPoint.getIndexSize() != null ) {
                        statisticsManager.setIndexSize( tableId, dataPoint.getIndexSize() );
                    }
                }
            } else {
                for ( long id : values ) {
                    if ( catalog.checkIfExistsTable( id ) ) {
                        statisticsManager.setTableCalls( id, dataPoint.getMonitoringType() );
                    }
                }
            }
        }
    }


    private void updateDmlStatistics( DmlDataPoint dataPoint, StatisticsManager<?> statisticsManager ) {
        if ( dataPoint.getChangedValues() != null ) {
            Set<Long> values = new HashSet<>( dataPoint.getAvailableColumnsWithTable().values() );
            boolean isOneTable = values.size() == 1;

            Catalog catalog = Catalog.getInstance();
            if ( isOneTable ) {
                long tableId = values.stream().findFirst().get();
                statisticsManager.setTableCalls( tableId, dataPoint.getMonitoringType() );

                if ( catalog.checkIfExistsTable( tableId ) ) {
                    if ( dataPoint.getMonitoringType().equals( "INSERT" ) ) {
                        int added = dataPoint.getRowCount();
                        statisticsManager.tablesToUpdate(
                                tableId,
                                dataPoint.getChangedValues(),
                                dataPoint.getMonitoringType(),
                                catalog.getTable( tableId ).schemaId );
                        statisticsManager.updateRowCountPerTable( tableId, added, dataPoint.getMonitoringType() );
                    } else if ( dataPoint.getMonitoringType().equals( "DELETE" ) ) {
                        int deleted = dataPoint.getRowCount();
                        statisticsManager.updateRowCountPerTable( tableId, deleted, dataPoint.getMonitoringType() );
                        // After a delete, it is not clear what exactly was deleted, so the statistics of the table are updated
                        statisticsManager.tablesToUpdate( tableId );
                    }
                }
            } else {
                for ( long id : values ) {
                    if ( catalog.checkIfExistsTable( id ) ) {
                        statisticsManager.setTableCalls( id, dataPoint.getMonitoringType() );
                    }

                }
            }
        }
    }

}
