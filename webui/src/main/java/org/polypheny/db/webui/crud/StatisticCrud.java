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

package org.polypheny.db.webui.crud;

import com.google.gson.Gson;
import io.javalin.http.Context;
import java.sql.Timestamp;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.monitoring.statistics.StatisticTable;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.requests.UIRequest;

public class StatisticCrud {

    @Getter
    private static Crud crud;
    @Getter
    public boolean isActiveTracking = false;
    private final StatisticsManager<?> statisticsManager = StatisticsManager.getInstance();


    public StatisticCrud( Crud crud ) {
        StatisticCrud.crud = crud;
        registerStatisticObserver();
    }


    /**
     * Ensures that changes in the ConfigManger toggle the correctly
     */
    private void registerStatisticObserver() {
        this.isActiveTracking = RuntimeConfig.ACTIVE_TRACKING.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
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
                isActiveTracking = c.getBoolean() && RuntimeConfig.DYNAMIC_QUERYING.getBoolean();
            }
        };
        RuntimeConfig.ACTIVE_TRACKING.addObserver( observer );
        RuntimeConfig.DYNAMIC_QUERYING.addObserver( observer );
    }


    public void getTableStatistics( Context ctx ) {

        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        Long tableId;
        Long schemaId;

        try {
            schemaId = Catalog.getInstance().getSchema( 1, request.tableId.split( "\\." )[0] ).id;
            tableId = Catalog.getInstance().getTable( schemaId, request.tableId.split( "\\." )[1] ).id;

            StatisticTable test = (StatisticTable) statisticsManager.getTableStatistic( schemaId, tableId );
            ctx.json( test );

        } catch ( UnknownTableException | UnknownSchemaException e ) {
            e.printStackTrace();
        }

    }


    /**
     * Return all available statistics to the client
     */
    public void getStatistics( final Context ctx, Gson gsonExpose ) {
        if ( RuntimeConfig.DYNAMIC_QUERYING.getBoolean() ) {
            ctx.result( gsonExpose.toJson( statisticsManager.getStatisticSchemaMap() ) );
        } else {
            ctx.json( new ConcurrentHashMap<>() );
        }
    }


    public void getMonitoringInformation( final Context ctx ) {

        ctx.json( "result" );
        List<QueryDataPoint> queryData = MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPoint.class );
        List<DmlDataPoint> dmlData = MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class );


    }


    public void getDmlInformation( final Context ctx ) {
        List<DmlDataPoint> dmlData = MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class );
        TreeMap<Timestamp, Integer> infoRow = new TreeMap<>();
        Timestamp lastTimestamp = null;
        for ( DmlDataPoint dmlDataPoint : dmlData ) {
            Timestamp time = dmlDataPoint.getRecordedTimestamp();
            if ( infoRow.isEmpty() ) {
                infoRow.put( time, 1 );
                lastTimestamp = time;
            } else {
                if ( (lastTimestamp.getTime() - time.getTime()) < TimeUnit.SECONDS.toMillis( 30 ) ) {
                    int num = infoRow.remove( lastTimestamp );
                    infoRow.put( lastTimestamp, num + 1 );
                } else {
                    infoRow.put( time, 1 );
                    lastTimestamp = time;
                }
            }

        }
        ctx.json( infoRow );
    }

}
