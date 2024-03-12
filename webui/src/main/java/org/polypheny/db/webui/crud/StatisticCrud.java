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

package org.polypheny.db.webui.crud;

import io.javalin.http.Context;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.requests.UIRequest;

@Slf4j
public class StatisticCrud {

    @Getter
    private static Crud crud;
    @Getter
    private boolean activeTracking = false;


    public StatisticCrud( Crud crud ) {
        StatisticCrud.crud = crud;
        registerStatisticObserver();
    }


    /**
     * Ensures that changes in the ConfigManger toggle the correctly
     */
    private void registerStatisticObserver() {
        this.activeTracking = RuntimeConfig.ACTIVE_TRACKING.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
        ConfigListener observer = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                setConfig( c );
            }


            @Override
            public void restart( Config c ) {
                setConfig( c );
            }


            private void setConfig( Config c ) {
                activeTracking = c.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
            }
        };
        RuntimeConfig.ACTIVE_TRACKING.addObserver( observer );
        RuntimeConfig.DYNAMIC_QUERYING.addObserver( observer );
    }


    public void getTableStatistics( Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        LogicalTable table = Catalog.getInstance().getSnapshot().rel().getTable( request.entityId ).orElseThrow();

        ctx.json( StatisticsManager.getInstance().getEntityStatistic( table.namespaceId, table.id ) );
    }


    /**
     * Return all available statistics to the client
     */
    public void getStatistics( final Context ctx ) {
        if ( RuntimeConfig.DYNAMIC_QUERYING.getBoolean() ) {
            ctx.json( StatisticsManager.getInstance().getQualifiedStatisticMap() );
        } else {
            ctx.json( new ConcurrentHashMap<>() );
        }
    }


    /**
     * General information for the UI dashboard.
     */
    public void getDashboardInformation( Context ctx ) {
        ctx.json( StatisticsManager.getInstance().getDashboardInformation() );
    }


    /**
     * Information base on time interval for diagram shown on the UI dashboard.
     */
    public void getDashboardDiagram( Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );
        String selectInterval = request.selectInterval;
        List<QueryDataPointImpl> queryData = MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPointImpl.class );
        List<DmlDataPoint> dmlData = MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class );
        TreeMap<Timestamp, Pair<Integer, Integer>> eachInfo;

        Timestamp endTime = new Timestamp( System.currentTimeMillis() );
        Timestamp startTimeAll;

        if ( !queryData.isEmpty() && !dmlData.isEmpty() ) {
            startTimeAll = (queryData.get( queryData.size() - 1 ).getRecordedTimestamp().getTime() < dmlData.get( dmlData.size() - 1 ).getRecordedTimestamp().getTime()) ? queryData.get( queryData.size() - 1 ).getRecordedTimestamp() : dmlData.get( dmlData.size() - 1 ).getRecordedTimestamp();
        } else if ( !dmlData.isEmpty() ) {
            startTimeAll = dmlData.get( dmlData.size() - 1 ).getRecordedTimestamp();
        } else if ( !queryData.isEmpty() ) {
            startTimeAll = queryData.get( queryData.size() - 1 ).getRecordedTimestamp();
        } else {
            ctx.json( new TreeMap<>() );
            return;
        }

        if ( NumberUtils.isCreatable( selectInterval ) ) {
            int interval = Integer.parseInt( selectInterval );
            eachInfo = getDashboardInfo( calculateStartTime( interval, endTime ), endTime, convertIntervalMinuteToLong( interval ), queryData, dmlData );
        } else {
            eachInfo = getDashboardInfo( startTimeAll, endTime, calculateIntervalAll( startTimeAll, endTime ), queryData, dmlData );
        }

        ctx.json( eachInfo );
    }


    private long calculateIntervalAll( Timestamp startTime, Timestamp endTime ) {
        return (endTime.getTime() - startTime.getTime()) / 10;
    }


    private long convertIntervalMinuteToLong( int minutes ) {
        return TimeUnit.MINUTES.toMillis( minutes );
    }


    private Timestamp calculateStartTime( int intervalInMinutes, Timestamp endTime ) {
        long startTime = endTime.getTime();
        for ( int numberOfInterval = 10; numberOfInterval > 0; numberOfInterval-- ) {
            startTime = startTime - convertIntervalMinuteToLong( intervalInMinutes );
        }
        return new Timestamp( startTime );
    }


    /**
     * Information for diagram shown on the UI dashboard.
     */
    private TreeMap<Timestamp, Pair<Integer, Integer>> getDashboardInfo( Timestamp startTime, Timestamp endTime, long interval, List<QueryDataPointImpl> queryData, List<DmlDataPoint> dmlData ) {
        TreeMap<Timestamp, Pair<Integer, Integer>> info = new TreeMap<>();
        TreeMap<Timestamp, Pair<Integer, Integer>> dashboardInfo = new TreeMap<>();
        boolean notInserted;

        Timestamp time = startTime;
        while ( endTime.getTime() - time.getTime() >= 0 ) {
            info.put( time, new Pair<>( 0, 0 ) );
            time = new Timestamp( time.getTime() + interval );
        }

        dashboardInfo.putAll( info );

        for ( QueryDataPointImpl queryDataPoint : queryData ) {
            notInserted = true;
            Timestamp queryTime = queryDataPoint.getRecordedTimestamp();
            for ( Entry<Timestamp, Pair<Integer, Integer>> timestampInfo : dashboardInfo.entrySet() ) {
                if ( timestampInfo.getKey().getTime() + interval - queryTime.getTime() > 0 && notInserted ) {
                    Pair<Integer, Integer> num = info.remove( timestampInfo.getKey() );
                    info.put( timestampInfo.getKey(), new Pair<>( num.left + 1, num.right ) );
                    notInserted = false;
                }
            }
        }

        dashboardInfo.clear();
        dashboardInfo.putAll( info );
        for ( DmlDataPoint dmlDataPoint : dmlData ) {
            notInserted = true;
            Timestamp dmlTime = dmlDataPoint.getRecordedTimestamp();
            for ( Entry<Timestamp, Pair<Integer, Integer>> timestampInfo : dashboardInfo.entrySet() ) {
                if ( timestampInfo.getKey().getTime() + interval - dmlTime.getTime() > 0 && notInserted ) {
                    Pair<Integer, Integer> num = info.remove( timestampInfo.getKey() );
                    info.put( timestampInfo.getKey(), new Pair<>( num.left, num.right + 1 ) );
                    notInserted = false;
                }
            }
        }
        return info;
    }

}
