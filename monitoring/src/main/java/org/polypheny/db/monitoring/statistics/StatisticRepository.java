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
        DdlDataPoint ddlDataPoint = dataPoint;
        if ( ddlDataPoint.getMonitoringType().equals( "TRUNCATE" ) ) {
            statisticsManager.updateRowCountPerTable( ddlDataPoint.getTableId(), 0, ddlDataPoint.getMonitoringType() );
            statisticsManager.tablesToUpdate( ddlDataPoint.getTableId(), null, ddlDataPoint.getMonitoringType(), ddlDataPoint.getSchemaId() );
        }
        if ( ddlDataPoint.getMonitoringType().equals( "DROP_TABLE" ) || ddlDataPoint.getMonitoringType().equals( "DROP_VIEW" ) || ddlDataPoint.getMonitoringType().equals( "DROP_MATERIALIZED_VIEW" ) ) {
            statisticsManager.deleteTableToUpdate( ddlDataPoint.getTableId(), ddlDataPoint.getSchemaId() );
        }
    }


    private void updateQueryStatistics( QueryDataPointImpl dataPoint, StatisticsManager<?> statisticsManager ) {
        QueryDataPointImpl dqlDataPoint = dataPoint;
        if ( !dqlDataPoint.getAvailableColumnsWithTable().isEmpty() ) {
            Set<Long> values = new HashSet<>( dqlDataPoint.getAvailableColumnsWithTable().values() );
            boolean isOneTable = values.size() == 1;
            Catalog catalog = Catalog.getInstance();

            if ( isOneTable ) {
                Long tableId = values.stream().findFirst().get();
                if ( catalog.checkIfExistsTable( tableId ) ) {
                    statisticsManager.setTableCalls( tableId, dqlDataPoint.getMonitoringType() );

                    // RowCount from UI is only used if there is no other possibility
                    if ( statisticsManager.rowCountPerTable( tableId ) == null || statisticsManager.rowCountPerTable( tableId ) == 0 ) {
                        statisticsManager.updateRowCountPerTable( tableId, dqlDataPoint.getRowCount(), "SET-ROW-COUNT" );
                    }

                    if ( dqlDataPoint.getIndexSize() != null ) {
                        statisticsManager.setIndexSize( tableId, dqlDataPoint.getIndexSize() );
                    }
                }
            } else {
                for ( Long id : values ) {
                    if ( catalog.checkIfExistsTable( id ) ) {
                        statisticsManager.setTableCalls( id, dqlDataPoint.getMonitoringType() );
                    }
                }
            }
        }
    }


    private void updateDmlStatistics( DmlDataPoint dataPoint, StatisticsManager<?> statisticsManager ) {
        DmlDataPoint dmlDataPoint = dataPoint;
        if ( dmlDataPoint.getChangedValues() != null ) {
            Set<Long> values = new HashSet<>( dmlDataPoint.getAvailableColumnsWithTable().values() );
            boolean isOneTable = values.size() == 1;

            Catalog catalog = Catalog.getInstance();
            if ( isOneTable ) {
                Long tableId = values.stream().findFirst().get();
                statisticsManager.setTableCalls( tableId, dmlDataPoint.getMonitoringType() );

                if ( catalog.checkIfExistsTable( tableId ) ) {
                    statisticsManager.tablesToUpdate( tableId, dmlDataPoint.getChangedValues(), dmlDataPoint.getMonitoringType(), catalog.getTable( tableId ).schemaId );

                    if ( dmlDataPoint.getMonitoringType().equals( "INSERT" ) ) {
                        int added = dmlDataPoint.getRowCount();
                        statisticsManager.updateRowCountPerTable( tableId, added, dmlDataPoint.getMonitoringType() );
                    } else if ( dmlDataPoint.getMonitoringType().equals( "DELETE" ) ) {
                        int deleted = dmlDataPoint.getRowCount();
                        statisticsManager.updateRowCountPerTable( tableId, deleted, dmlDataPoint.getMonitoringType() );
                        //after a delete it is not clear what exactly was deleted, so the statistics are updated
                        statisticsManager.tablesToUpdate( tableId );
                    }

                }
            } else {
                for ( Long id : values ) {
                    if ( catalog.checkIfExistsTable( id ) ) {
                        statisticsManager.setTableCalls( id, dmlDataPoint.getMonitoringType() );
                    }

                }
            }
        }
    }

}
