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


import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.CatalogImpl;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManagerImpl;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.exploreByExample.ExploreManager;
import org.polypheny.db.exploreByExample.ExploreQueryProcessor;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.HostInformation;
import org.polypheny.db.information.JavaInformation;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.core.MonitoringService;
//import org.polypheny.db.monitoring.obsolet.MonitoringService;
import org.polypheny.db.monitoring.obsolet.subscriber.DummySubscriber;
import org.polypheny.db.monitoring.obsolet.subscriber.InternalSubscriber;
import org.polypheny.db.monitoring.obsolet.subscriber.SubscriptionTopic;
import org.polypheny.db.processing.AuthenticatorImpl;
import org.polypheny.db.statistic.StatisticQueryProcessor;
import org.polypheny.db.statistic.StatisticsManager;
import org.polypheny.db.transaction.*;
import org.polypheny.db.util.FileSystemManager;
import org.polypheny.db.webui.ConfigServer;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.InformationServer;

import java.io.Serializable;


@Command(name = "polypheny-db", description = "Polypheny-DB command line hook.")
@Slf4j
public class PolyphenyDb {

    private PUID shutdownHookId;

    private final TransactionManager transactionManager = new TransactionManagerImpl();

    @Option(name = { "-resetCatalog" }, description = "Reset the catalog")
    public boolean resetCatalog = false;

    @Option(name = { "-memoryCatalog" }, description = "Store catalog only in-memory")
    public boolean memoryCatalog = false;

    @Option(name = { "-testMode" }, description = "Special catalog configuration for running tests")
    public boolean testMode = false;

    // required for unit tests to determine when the system is ready to process queries
    @Getter
    private volatile boolean isReady = false;


    @SuppressWarnings("unchecked")
    public static void main( final String[] args ) {
        try {
            if ( log.isDebugEnabled() ) {
                log.debug( "PolyphenyDb.main( {} )", java.util.Arrays.toString( args ) );
            }
            final SingleCommand<PolyphenyDb> parser = SingleCommand.singleCommand( PolyphenyDb.class );
            final PolyphenyDb polyphenyDb = parser.parse( args );

            polyphenyDb.runPolyphenyDb();

        } catch ( Throwable uncaught ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Uncaught Throwable.", uncaught );
            }
        }
    }


    public void runPolyphenyDb() throws GenericCatalogException {
        // Move data folder
        if ( FileSystemManager.getInstance().checkIfExists( "data.backup" ) ) {
            FileSystemManager.getInstance().recursiveDeleteFolder( "data" );
            if ( !FileSystemManager.getInstance().moveFolder( "data.backup", "data" ) ) {
                throw new RuntimeException( "Unable to restore data folder." );
            }
            log.info( "Restoring the data folder." );
        }

        // Reset data folder
        if ( resetCatalog ) {
            if ( !FileSystemManager.getInstance().recursiveDeleteFolder( "data" ) ) {
                log.error( "Unable to delete the data folder." );
            }
        }

        // Backup data folder (running in test mode / memory mode)
        if ( (testMode || memoryCatalog) && FileSystemManager.getInstance().checkIfExists( "data" ) ) {
            if ( !FileSystemManager.getInstance().moveFolder( "data", "data.backup" ) ) {
                throw new RuntimeException( "Unable to create the backup folder." );
            }
        }

        class ShutdownHelper implements Runnable {

            private final Serializable[] joinOnNotStartedLock = new Serializable[0];
            private volatile boolean alreadyRunning = false;
            private volatile boolean hasFinished = false;
            private volatile Thread executor = null;


            @Override
            public void run() {
                synchronized ( this ) {
                    if ( alreadyRunning ) {
                        return;
                    } else {
                        alreadyRunning = true;
                        executor = Thread.currentThread();
                    }
                }
                synchronized ( joinOnNotStartedLock ) {
                    joinOnNotStartedLock.notifyAll();
                }

                synchronized ( this ) {
                    hasFinished = true;
                }
            }


            public boolean hasFinished() {
                synchronized ( this ) {
                    return hasFinished;
                }
            }


            public void join( final long millis ) throws InterruptedException {
                synchronized ( joinOnNotStartedLock ) {
                    while ( alreadyRunning == false ) {
                        joinOnNotStartedLock.wait( 0 );
                    }
                }
                if ( executor != null ) {
                    executor.join( millis );
                }
            }

        }

        final ShutdownHelper sh = new ShutdownHelper();
        // shutdownHookId = addShutdownHook( "Component Terminator", sh );

        final ConfigServer configServer = new ConfigServer( RuntimeConfig.CONFIG_SERVER_PORT.getInteger() );
        final InformationServer informationServer = new InformationServer( RuntimeConfig.INFORMATION_SERVER_PORT.getInteger() );

        try {
            new JavaInformation();
        } catch ( Exception e ) {
            log.error( "Unable to retrieve java runtime information." );
        }
        try {
            new HostInformation();
        } catch ( Exception e ) {
            log.error( "Unable to retrieve host information." );
        }
        try{
            //TODO add storage backend connector form Runtime Config instead of specifying it in Monitoring Service
            //final MonitoringService monitoringService = new MonitoringService();
        } catch( Exception e) {
            log.error( "Unable to connect to monitoring service client" );
        }

        /*ThreadManager.getComponent().addShutdownHook( "[ShutdownHook] HttpServerDispatcher.stop()", () -> {
            try {
                httpServerDispatcher.stop();
            } catch ( Exception e ) {
                GLOBAL_LOGGER.warn( "Exception during HttpServerDispatcher shutdown", e );
            }
        } );*/

        final Authenticator authenticator = new AuthenticatorImpl();

        // Initialize interface manager
        QueryInterfaceManager.initialize( transactionManager, authenticator );

        // Startup and restore catalog
        Catalog catalog;
        Transaction trx = null;
        try {
            Catalog.resetCatalog = resetCatalog;
            Catalog.memoryCatalog = memoryCatalog;
            Catalog.testMode = testMode;
            catalog = Catalog.setAndGetInstance( new CatalogImpl() );
            trx = transactionManager.startTransaction( "pa", "APP", false, "Catalog Startup" );
            AdapterManager.getInstance().restoreAdapters();
            QueryInterfaceManager.getInstance().restoreInterfaces( catalog );
            trx.commit();
            trx = transactionManager.startTransaction( "pa", "APP", false, "Catalog Startup" );
            catalog.restoreColumnPlacements( trx );
            trx.commit();
        } catch ( UnknownDatabaseException | UnknownUserException | UnknownSchemaException | TransactionException e ) {
            if ( trx != null ) {
                try {
                    trx.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Error while rolling back the transaction", e );
                }
            }
            throw new RuntimeException( "Something went wrong while restoring stores from the catalog.", e );
        }

        // Initialize DdlManager
        DdlManager.setAndGetInstance( new DdlManagerImpl( catalog ) );

        // Start Polypheny UI
        final HttpServer httpServer = new HttpServer( transactionManager, authenticator );
        Thread polyphenyUiThread = new Thread( httpServer );
        polyphenyUiThread.start();
        try {
            polyphenyUiThread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }

        // Create internal query interfaces
        final StatisticQueryProcessor statisticQueryProcessor = new StatisticQueryProcessor( transactionManager, authenticator );
        StatisticsManager<?> statisticsManager = StatisticsManager.getInstance();
        statisticsManager.setSqlQueryInterface( statisticQueryProcessor );

        // Initialize index manager
        try {
            IndexManager.getInstance().initialize( transactionManager );
            IndexManager.getInstance().restoreIndexes();
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException | UnknownTableException | TransactionException | UnknownKeyException e ) {
            throw new RuntimeException( "Something went wrong while initializing index manager.", e );
        }

        // Call DockerManager once to remove old containers
        DockerManager.getInstance();

        final ExploreQueryProcessor exploreQueryProcessor = new ExploreQueryProcessor( transactionManager, authenticator ); // Explore-by-Example
        ExploreManager explore = ExploreManager.getInstance();
        explore.setExploreQueryProcessor( exploreQueryProcessor );


        // Todo remove this testing
       /* InternalSubscriber internalSubscriber = new InternalSubscriber();
        DummySubscriber dummySubscriber = new DummySubscriber();
        MonitoringService.INSTANCE.subscribeToEvents( internalSubscriber, SubscriptionTopic.TABLE, 6, "Internal Usage" );
        MonitoringService.INSTANCE.subscribeToEvents( internalSubscriber, SubscriptionTopic.STORE, 2, "Internal Usage" );
        MonitoringService.INSTANCE.subscribeToEvents( dummySubscriber, SubscriptionTopic.TABLE, 6, "Lorem ipsum" );
        *///

        MonitoringService monitoringService = MonitoringServiceProvider.MONITORING_SERVICE();


        //


        log.info( "****************************************************************************************************" );
        log.info( "                Polypheny-DB successfully started and ready to process your queries!" );
        log.info( "                              The UI is waiting for you on port {}:", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        log.info( "                                       http://localhost:{}", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        log.info( "                              UI for Monitoring with influxDB");
        log.info( "                                       http://localhost:8086");
        log.info( "****************************************************************************************************" );
        isReady = true;

        try {
            log.trace( "Waiting for the Shutdown-Hook to finish ..." );
            sh.join( 0 ); // "forever"
            if ( sh.hasFinished() == false ) {
                log.warn( "The Shutdown-Hook has not finished execution, but join() returned ..." );
            } else {
                log.info( "Waiting for the Shutdown-Hook to finish ... done." );
            }
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted while waiting for the Shutdown-Hook to finish. The JVM might terminate now without having terminate() on all components invoked.", e );
        }
    }

}
