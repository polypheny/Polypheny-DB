/*
 * Copyright 2019-2020 The Polypheny Project
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


import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.CatalogManagerImpl;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.information.HostInformation;
import org.polypheny.db.information.JavaInformation;
import org.polypheny.db.jdbc.JdbcInterface;
import org.polypheny.db.processing.AuthenticatorImpl;
import org.polypheny.db.statistic.StatisticQueryProcessor;
import org.polypheny.db.statistic.StatisticsManager;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.webui.ConfigServer;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.InformationServer;


@Slf4j
public class PolyphenyDb {

    private PUID shutdownHookId;

    private final TransactionManager transactionManager = new TransactionManagerImpl();


    @SuppressWarnings("unchecked")
    public static void main( final String[] args ) {
        try {
            if ( log.isDebugEnabled() ) {
                log.debug( "PolyphenyDb.main( {} )", java.util.Arrays.toString( args ) );
            }

            new PolyphenyDb().runPolyphenyDb();

        } catch ( Throwable uncaught ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Uncaught Throwable.", uncaught );
            }
        }
    }


    public void runPolyphenyDb() throws GenericCatalogException {

        Catalog catalog;
        Transaction trx = null;
        try {
            trx = transactionManager.startTransaction( "pa", "APP", false );
            catalog = CatalogManagerImpl.getInstance().getCatalog( trx.getXid() );
            StoreManager.getInstance().restoreStores( catalog );
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

        new JavaInformation();
        new HostInformation();

        /*ThreadManager.getComponent().addShutdownHook( "[ShutdownHook] HttpServerDispatcher.stop()", () -> {
            try {
                httpServerDispatcher.stop();
            } catch ( Exception e ) {
                GLOBAL_LOGGER.warn( "Exception during HttpServerDispatcher shutdown", e );
            }
        } );*/

        final Authenticator authenticator = new AuthenticatorImpl();
        final JdbcInterface jdbcInterface = new JdbcInterface( transactionManager, authenticator );
        final HttpServer httpServer = new HttpServer( transactionManager, authenticator, RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        final StatisticQueryProcessor statisticQueryProcessor = new StatisticQueryProcessor( transactionManager, authenticator );

        Thread jdbcInterfaceThread = new Thread( jdbcInterface );
        jdbcInterfaceThread.start();

        Thread webUiInterfaceThread = new Thread( httpServer );
        webUiInterfaceThread.start();




        try {
            jdbcInterfaceThread.join();
            webUiInterfaceThread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }

        StatisticsManager store = StatisticsManager.getInstance();
        store.setSqlQueryInterface( statisticQueryProcessor );

        log.info( "****************************************************************************************************" );
        log.info( "                Polypheny-DB successfully started and ready to process your queries!" );
        log.info( "                           The UI is waiting for you on port: {}", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        log.info( "****************************************************************************************************" );

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
