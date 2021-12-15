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

package org.polypheny.db.monitoring.statistics;

import java.util.HashSet;
import java.util.Set;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringDataPoint.DataPointType;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.monitoring.repository.MonitoringRepository;

public class StatisticRepository implements MonitoringRepository {

    @Override
    public void dataPoint( MonitoringDataPoint dataPoint ) {
        StatisticsManager<?> statisticsManager = StatisticsManager.getInstance();
        if ( dataPoint.getDataPointType() == DataPointType.DML ) {
            DmlDataPoint dmlDataPoint = ((DmlDataPoint) dataPoint);
            if ( dmlDataPoint.getChangedValues() != null ) {
                Set<Long> values = new HashSet<>( dmlDataPoint.getAvailableColumnsWithTable().values() );
                boolean isOneTable = values.size() == 1;
                Long tableId = values.stream().findFirst().get();
                Catalog catalog = Catalog.getInstance();
                if ( isOneTable ) {
                    statisticsManager.setTableCalls( tableId, dmlDataPoint.getMonitoringType() );

                    if ( catalog.checkIfExistsTable( tableId ) ) {
                        statisticsManager.tablesToUpdate( tableId, dmlDataPoint.getChangedValues(), dmlDataPoint.getMonitoringType() );

                        if ( dmlDataPoint.getMonitoringType().equals( "INSERT" ) ) {
                            int added = dmlDataPoint.getRowCount();
                            statisticsManager.updateRowCountPerTable( tableId, added, true );
                        } else if ( dmlDataPoint.getMonitoringType().equals( "DELETE" ) ) {
                            int deleted = dmlDataPoint.getRowCount();
                            statisticsManager.updateRowCountPerTable( tableId, deleted, false );
                        }

                    }
                }
            }


        } else if ( dataPoint.getDataPointType() == DataPointType.QueryDataPointImpl ) {
            QueryDataPointImpl dqlDataPoint = ((QueryDataPointImpl) dataPoint);
            if ( !dqlDataPoint.getAvailableColumnsWithTable().isEmpty() ) {
                Set<Long> values = new HashSet<>( dqlDataPoint.getAvailableColumnsWithTable().values() );
                boolean isOneTable = values.size() == 1;
                Catalog catalog = Catalog.getInstance();

                if ( isOneTable ) {
                    Long tableId = values.stream().findFirst().get();
                    if ( catalog.checkIfExistsTable( tableId ) ) {
                        statisticsManager.setTableCalls( tableId, dqlDataPoint.getMonitoringType() );

                        if ( dqlDataPoint.getIndexSize() != null ) {
                            statisticsManager.setIndexSize( tableId, dqlDataPoint.getIndexSize() );
                        }
                    }
                } else{
                    for ( Long id : values ) {
                        if ( catalog.checkIfExistsTable( id ) ) {
                            statisticsManager.setTableCalls( id, dqlDataPoint.getMonitoringType() );
                        }

                    }
                }
            }
        }


    }

}
