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

package org.polypheny.db.information;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.information.HostInformation;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.db.processing.caching.RoutingPlanCache;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.VersionCollector;

public class StatusService {

    private static StatusService INSTANCE = null;

    public static final String PREFIX_KEY = "/status";
    private final TransactionManager transactionManager;


    public static StatusService getInstance() {
        if ( INSTANCE == null ) {
            throw new GenericRuntimeException( "StatusService not initialized yet" );
        }
        return INSTANCE;
    }


    public static void initialize( TransactionManager manager, Javalin webuiServer ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "StatusService already initialized" );
        }
        INSTANCE = new StatusService( manager, webuiServer );
    }


    private StatusService( TransactionManager manager, Javalin webuiServer ) {
        this.transactionManager = manager;

        registerStatusRoutes( webuiServer );
    }


    private void registerStatusRoutes( Javalin webuiServer ) {

        webuiServer.get( PREFIX_KEY + "/uuid", this::getUuid );

        webuiServer.get( PREFIX_KEY + "/version", this::getVersion );

        webuiServer.get( PREFIX_KEY + "/hash", this::getHash );

        webuiServer.get( PREFIX_KEY + "/memory-current", this::getCurrentMemory );

        webuiServer.get( PREFIX_KEY + "/transactions-active", this::getActiveTransactions );

        webuiServer.get( PREFIX_KEY + "/transactions-since-restart", this::getTransactionRestart );

        webuiServer.get( PREFIX_KEY + "/cache-implementation", this::getImplementationCacheSize );

        webuiServer.get( PREFIX_KEY + "/cache-queryplan", this::getQueryPlanCacheSize );

        webuiServer.get( PREFIX_KEY + "/cache-routingplan", this::getRoutingPlanCacheSize );

        webuiServer.get( PREFIX_KEY + "/monitoring-queue", this::getMonitoringQueueSize );

    }


    private void getTransactionRestart( Context context ) {
        context.result( String.valueOf( transactionManager.getNumberOfTotalTransactions() ) );
    }


    private void getMonitoringQueueSize( Context context ) {
        context.result( String.valueOf( MonitoringServiceProvider.getInstance().getSize() ) );
    }


    private void getRoutingPlanCacheSize( Context context ) {
        context.result( String.valueOf( RoutingPlanCache.INSTANCE.getSize() ) );
    }


    private void getQueryPlanCacheSize( Context context ) {
        context.result( String.valueOf( QueryPlanCache.INSTANCE.getSize() ) );
    }


    public void getUuid( Context context ) {
        context.result( RuntimeConfig.INSTANCE_UUID.getString() );
    }


    public void getVersion( Context context ) {
        context.result( VersionCollector.INSTANCE.getVersion() );
    }


    public void getHash( Context context ) {
        context.result( VersionCollector.INSTANCE.getHash() );
    }


    public void getCurrentMemory( Context context ) {
        context.result( String.valueOf( HostInformation.getINSTANCE().getUsedFreeProvider().get().left ) );
    }


    public void getActiveTransactions( Context context ) {
        context.result( String.valueOf( transactionManager.getNumberOfActiveTransactions() ) );
    }


    public void getImplementationCacheSize( Context context ) {
        context.result( String.valueOf( ImplementationCache.INSTANCE.getCacheSize() ) );
    }

}
