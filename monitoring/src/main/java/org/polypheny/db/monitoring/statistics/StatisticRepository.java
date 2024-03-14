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

package org.polypheny.db.monitoring.statistics;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringDataPoint.DataPointType;
import org.polypheny.db.monitoring.events.MonitoringType;
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
        StatisticsManager statisticsManager = StatisticsManager.getInstance();
        statisticsManager.updateCommitRollback( dataPoint.isCommitted() );

        if ( dataPoint.getDataPointType() == DataPointType.DML ) {
            updateDmlStatistics( (DmlDataPoint) dataPoint, statisticsManager );
        } else if ( dataPoint.getDataPointType() == DataPointType.QueryDataPointImpl ) {
            updateQueryStatistics( (QueryDataPointImpl) dataPoint, statisticsManager );
        } else if ( dataPoint.getDataPointType() == DataPointType.DDL ) {
            updateDdlStatistics( (DdlDataPoint) dataPoint, statisticsManager );
        }
    }


    private void updateDdlStatistics( DdlDataPoint dataPoint, StatisticsManager statisticsManager ) {
        if ( dataPoint.getMonitoringType() == MonitoringType.TRUNCATE ) {
            statisticsManager.updateRowCountPerEntity(
                    dataPoint.getTableId(),
                    0,
                    dataPoint.getMonitoringType() );
            statisticsManager.entitiesToUpdate(
                    dataPoint.getTableId(),
                    null,
                    dataPoint.getMonitoringType(),
                    dataPoint.getNamespaceId() );
        }
        if ( dataPoint.getMonitoringType() == MonitoringType.DROP_TABLE ) {
            statisticsManager.deleteEntityToUpdate( dataPoint.getTableId() );
        }
        if ( dataPoint.getMonitoringType() == MonitoringType.DROP_COLUMN ) {
            statisticsManager.entitiesToUpdate(
                    dataPoint.getTableId(),
                    Collections.singletonMap( dataPoint.getColumnId(), null ),
                    dataPoint.getMonitoringType(),
                    dataPoint.getNamespaceId() );
        }
    }


    private void updateQueryStatistics( QueryDataPointImpl dataPoint, StatisticsManager statisticsManager ) {
        if ( !dataPoint.getAvailableColumnsWithTable().isEmpty() ) {
            Set<Long> values = new HashSet<>( dataPoint.getAvailableColumnsWithTable().values() );
            boolean isOneTable = values.size() == 1;
            Catalog catalog = Catalog.getInstance();

            if ( isOneTable ) {
                long tableId = values.stream().findFirst().get();
                if ( catalog.getSnapshot().getLogicalEntity( tableId ).isPresent() ) {
                    statisticsManager.setEntityCalls( tableId, dataPoint.getMonitoringType() );

                    // RowCount from UI is only used if there is no other possibility
                    if ( statisticsManager.tupleCountPerEntity( tableId ) == null || statisticsManager.tupleCountPerEntity( tableId ) == 0 ) {
                        statisticsManager.updateRowCountPerEntity( tableId, dataPoint.getRowCount(), MonitoringType.SET_ROW_COUNT );
                    }

                    if ( dataPoint.getIndexSize() != null ) {
                        statisticsManager.setIndexSize( tableId, dataPoint.getIndexSize() );
                    }
                }
            } else {
                for ( long id : values ) {
                    if ( catalog.getSnapshot().getLogicalEntity( id ).isPresent() ) {
                        statisticsManager.setEntityCalls( id, dataPoint.getMonitoringType() );
                    }
                }
            }
        }
    }


    private void updateDmlStatistics( DmlDataPoint dataPoint, StatisticsManager statisticsManager ) {
        if ( dataPoint.getChangedValues() == null ) {
            return;
        }

        Set<Long> values = new HashSet<>( dataPoint.getAvailableColumnsWithTable().values() );
        boolean isOneTable = values.size() == 1;

        Catalog catalog = Catalog.getInstance();
        if ( isOneTable ) {
            long tableId = values.stream().findFirst().get();
            statisticsManager.setEntityCalls( tableId, dataPoint.getMonitoringType() );

            if ( catalog.getSnapshot().getLogicalEntity( tableId ).isEmpty() ) {
                return;
            }
            if ( dataPoint.getMonitoringType() == MonitoringType.INSERT ) {
                long added = dataPoint.getRowCount();
                statisticsManager.entitiesToUpdate(
                        tableId,
                        dataPoint.getChangedValues(),
                        dataPoint.getMonitoringType(),
                        catalog.getSnapshot().getLogicalEntity( tableId ).orElseThrow().namespaceId );
                statisticsManager.updateRowCountPerEntity( tableId, added, dataPoint.getMonitoringType() );
            } else if ( dataPoint.getMonitoringType() == MonitoringType.DELETE ) {
                long deleted = dataPoint.getRowCount();
                statisticsManager.updateRowCountPerEntity( tableId, deleted, dataPoint.getMonitoringType() );
                // After a delete, it is not clear what exactly was deleted, so the statistics of the table are updated
                statisticsManager.entitiesToUpdate( tableId );
            }
        } else {
            for ( long id : values ) {
                if ( catalog.getSnapshot().getLogicalEntity( id ).isPresent() ) {
                    statisticsManager.setEntityCalls( id, dataPoint.getMonitoringType() );
                }

            }
        }

    }

}
