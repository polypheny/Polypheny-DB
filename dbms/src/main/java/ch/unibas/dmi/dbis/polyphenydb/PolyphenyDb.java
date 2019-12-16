/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.information.HostInformation;
import ch.unibas.dmi.dbis.polyphenydb.information.JavaInformation;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JdbcInterface;
import ch.unibas.dmi.dbis.polyphenydb.processing.AuthenticatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.processing.TransactionManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.webui.ConfigServer;
import ch.unibas.dmi.dbis.polyphenydb.webui.HttpServer;
import ch.unibas.dmi.dbis.polyphenydb.webui.InformationServer;
import ch.unibas.dmi.dbis.polyphenydb.processing.SqlQueryInterface;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;


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
        try {
            catalog = CatalogManagerImpl.getInstance().getCatalog( transactionManager.startTransaction( "pa", "APP", false ).getXid() );
            StoreManager.getInstance().restoreStores( catalog );
        } catch ( UnknownDatabaseException | UnknownUserException | UnknownSchemaException e ) {
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
        // prolly overkill just for testing
        final SqlQueryInterface sqlQuery = new SqlQueryInterface( transactionManager, authenticator );

        Thread jdbcInterfaceThread = new Thread( jdbcInterface );
        jdbcInterfaceThread.start();

        Thread webUiInterfaceThread = new Thread( httpServer );
        webUiInterfaceThread.start();

        Thread sqlQueryThread = new Thread( sqlQuery );
        sqlQueryThread.start();


        try {
            jdbcInterfaceThread.join();
            webUiInterfaceThread.join();
            sqlQueryThread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }

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
