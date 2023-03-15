/*
 * Copyright 2019-2023 The Polypheny Project
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

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import java.awt.SystemTray;
import java.io.File;
import java.io.Serializable;
import java.util.List;
import javax.inject.Inject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.StatusService.ErrorConfig;
import org.polypheny.db.StatusService.StatusType;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManagerImpl;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.gui.GuiUtils;
import org.polypheny.db.gui.SplashHelper;
import org.polypheny.db.gui.TrayGui;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.HostInformation;
import org.polypheny.db.information.JavaInformation;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.statistics.StatisticQueryProcessor;
import org.polypheny.db.monitoring.statistics.StatisticsManagerImpl;
import org.polypheny.db.partition.FrequencyMap;
import org.polypheny.db.partition.FrequencyMapImpl;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.PartitionManagerFactoryImpl;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.AuthenticatorImpl;
import org.polypheny.db.processing.ConstraintEnforceAttacher.ConstraintTracker;
import org.polypheny.db.processing.JsonRelProcessorImpl;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.view.MaterializedViewManager;
import org.polypheny.db.view.MaterializedViewManagerImpl;
import org.polypheny.db.webui.ConfigServer;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.InformationServer;
import org.polypheny.db.webui.UiTestingConfigPage;
import org.polypheny.db.webui.UiTestingMonitoringPage;


@Command(name = "polypheny-db", description = "Polypheny-DB command line hook.")
@Slf4j
public class PolyphenyDb {

    private PUID shutdownHookId;

    private final TransactionManager transactionManager = TransactionManagerImpl.getInstance();

    @Inject
    public HelpOption<?> helpOption;

    @Option(name = { "-resetCatalog" }, description = "Reset the catalog")
    public boolean resetCatalog = false;

    @Option(name = { "-resetDocker" }, description = "Removes all Docker instances, which are from previous Polypheny runs.")
    public boolean resetDocker = false;

    @Option(name = { "-resetPlugins" }, description = "Removes all plugins from the plugins/ folder in the Polypheny Home directory.")
    public boolean resetPlugins = false;

    @Option(name = { "-memoryCatalog" }, description = "Store catalog only in-memory")
    public boolean memoryCatalog = false;

    @Option(name = { "-testMode" }, description = "Special catalog configuration for running tests")
    public boolean testMode = false;

    @Option(name = { "-gui" }, description = "Show splash screen on startup and add taskbar gui")
    public boolean desktopMode = false;

    @Option(name = { "-daemon" }, description = "Disable splash screen")
    public boolean daemonMode = false;

    @Option(name = { "-defaultStore" }, description = "Type of default store")
    public String defaultStoreName = "hsqldb";

    @Option(name = { "-defaultSource" }, description = "Type of default source")
    public String defaultSourceName = "csv";

    @Option(name = { "-c", "--config" }, description = "Path to the configuration file", type = OptionType.GLOBAL)
    protected String applicationConfPath;

    @Option(name = { "-v", "--version" }, description = "Current version of Polypheny-DB")
    public boolean versionOptionEnabled = false;

    // required for unit tests to determine when the system is ready to process queries
    @Getter
    private volatile boolean isReady = false;
    private SplashHelper splashScreen;
    private Catalog catalog;


    public static void main( final String[] args ) {
        try {
            if ( log.isDebugEnabled() ) {
                log.debug( "PolyphenyDb.main( {} )", java.util.Arrays.toString( args ) );
            }
            final SingleCommand<PolyphenyDb> parser = SingleCommand.singleCommand( PolyphenyDb.class );
            final PolyphenyDb polyphenyDb = parser.parse( args );

            StatusService.addSubscriber( log::info, StatusType.INFO );
            StatusService.addSubscriber( log::error, StatusType.ERROR );

            // Hide dock icon on macOS systems
            System.setProperty( "apple.awt.UIElement", "true" );

            if ( polyphenyDb.helpOption.showHelpIfRequested() ) {
                return;
            }

            if ( polyphenyDb.versionOptionEnabled ) {
                System.out.println( "v" + polyphenyDb.getClass().getPackage().getImplementationVersion() );
                return;
            }

            polyphenyDb.runPolyphenyDb();
        } catch ( Throwable uncaught ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Uncaught Throwable.", uncaught );
                StatusService.printError(
                        "Error: " + uncaught.getMessage(),
                        ErrorConfig.builder().func( ErrorConfig.DO_NOTHING ).doExit( true ).showButton( true ).buttonMessage( "Exit" ).build() );
            }
        }
    }


    public void runPolyphenyDb() throws GenericCatalogException {
        if ( resetDocker ) {
            log.warn( "[-resetDocker] option is set, this option is only for development." );
        }

        // Configuration shall not be persisted
        ConfigManager.memoryMode = (testMode || memoryCatalog);
        ConfigManager.resetCatalogOnStartup = resetCatalog;

        // Select behavior depending on arguments
        boolean showSplashScreen;
        boolean trayMenu;
        boolean openUiInBrowser;
        if ( daemonMode ) {
            showSplashScreen = false;
            trayMenu = false;
            openUiInBrowser = false;
        } else if ( desktopMode ) {
            showSplashScreen = true;
            try {
                trayMenu = SystemTray.isSupported();
            } catch ( Exception e ) {
                trayMenu = false;
            }
            openUiInBrowser = true;
        } else {
            showSplashScreen = false;
            trayMenu = false;
            openUiInBrowser = false;
        }

        // Open splash screen
        if ( showSplashScreen ) {
            this.splashScreen = new SplashHelper();
        }

        // Check if Polypheny is already running
        if ( GuiUtils.checkPolyphenyAlreadyRunning() ) {
            if ( openUiInBrowser ) {
                GuiUtils.openUiInBrowser();
            }
            System.err.println( "There is already an instance of Polypheny running on this system." );
            System.exit( 0 );
        }

        // Restore data folder
        if ( PolyphenyHomeDirManager.getInstance().checkIfExists( "data.backup" ) ) {
            PolyphenyHomeDirManager.getInstance().recursiveDeleteFolder( "data" );
            if ( !PolyphenyHomeDirManager.getInstance().moveFolder( "data.backup", "data" ) ) {
                throw new RuntimeException( "Unable to restore data folder." );
            }
            log.info( "Restoring the data folder." );
        }

        // Reset catalog, data and configuration
        if ( resetCatalog ) {
            if ( !PolyphenyHomeDirManager.getInstance().recursiveDeleteFolder( "data" ) ) {
                log.error( "Unable to delete the data folder." );
            }
            ConfigManager.getInstance().resetDefaultConfiguration();
        }

        // Backup data folder (running in test mode / memory mode)
        if ( (testMode || memoryCatalog) && PolyphenyHomeDirManager.getInstance().checkIfExists( "data" ) ) {
            if ( !PolyphenyHomeDirManager.getInstance().moveFolder( "data", "data.backup" ) ) {
                throw new RuntimeException( "Unable to create the backup folder." );
            }
        }

        // Enables Polypheny to be started with a different config.
        // Otherwise, Config at default location is used.
        if ( applicationConfPath != null && PolyphenyHomeDirManager.getInstance().checkIfExists( applicationConfPath ) ) {
            ConfigManager.getInstance();
            ConfigManager.setApplicationConfFile( new File( applicationConfPath ) );
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
                    while ( !alreadyRunning ) {
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

        // Start config server and information server
        new ConfigServer( RuntimeConfig.CONFIG_SERVER_PORT.getInteger() );
        new InformationServer( RuntimeConfig.INFORMATION_SERVER_PORT.getInteger() );

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

        // Initialize plugin manager
        PolyPluginManager.init( resetPlugins );

        // Initialize statistic manager
        final StatisticQueryProcessor statisticQueryProcessor = new StatisticQueryProcessor( transactionManager, authenticator );
        StatisticsManager.setAndGetInstance( new StatisticsManagerImpl<>( statisticQueryProcessor ) );

        // Initialize MaterializedViewManager
        MaterializedViewManager.setAndGetInstance( new MaterializedViewManagerImpl( transactionManager ) );

        // Startup and restore catalog
        Transaction trx = null;
        try {
            Catalog.resetCatalog = resetCatalog;
            Catalog.memoryCatalog = memoryCatalog;
            Catalog.testMode = testMode;
            Catalog.resetDocker = resetDocker;
            Catalog.defaultStore = Adapter.fromString( defaultStoreName, AdapterType.STORE );
            Catalog.defaultSource = Adapter.fromString( defaultSourceName, AdapterType.SOURCE );
            catalog = PolyPluginManager.getCATALOG_SUPPLIER().get();
            if ( catalog == null ) {
                throw new RuntimeException( "There was no catalog submitted, aborting." );
            }

            trx = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Catalog Startup" );
            AdapterManager.getInstance().restoreAdapters();
            loadDefaults();
            QueryInterfaceManager.getInstance().restoreInterfaces( catalog );
            trx.commit();
            trx = transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Catalog Startup" );
            catalog.restoreColumnPlacements( trx );
            catalog.restoreViews( trx );
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

        // Initialize DDL Manager
        DdlManager.setAndGetInstance( new DdlManagerImpl( catalog ) );

        // Initialize PartitionMangerFactory
        PartitionManagerFactory.setAndGetInstance( new PartitionManagerFactoryImpl() );
        FrequencyMap.setAndGetInstance( new FrequencyMapImpl( catalog ) );

        // Initialize statistic settings
        StatisticsManager.getInstance().initializeStatisticSettings();

        // Start Polypheny-UI
        final HttpServer httpServer = new HttpServer( transactionManager, authenticator );
        Thread polyphenyUiThread = new Thread( httpServer );
        polyphenyUiThread.start();
        try {
            polyphenyUiThread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }

        // temporary add sql and rel here
        LanguageManager.getINSTANCE().addQueryLanguage(
                NamespaceType.RELATIONAL,
                "rel",
                List.of( "rel", "relational" ),
                null,
                JsonRelProcessorImpl::new,
                null );

        // Initialize index manager
        try {
            IndexManager.getInstance().initialize( transactionManager );
            IndexManager.getInstance().restoreIndexes();
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException | UnknownTableException | TransactionException | UnknownKeyException e ) {
            throw new RuntimeException( "Something went wrong while initializing index manager.", e );
        }

        // Call DockerManager once to remove old containers
        DockerManager.getInstance();

        // Add config and monitoring test page for UI testing
        if ( testMode ) {
            new UiTestingConfigPage();
            new UiTestingMonitoringPage();
        }

        // Start monitoring service
        MonitoringServiceProvider.resetRepository = resetCatalog;
        MonitoringServiceProvider.getInstance();

        // Add icon to system tray
        if ( trayMenu ) {
            // Init TrayGUI
            TrayGui.getInstance();
        }

        PolyPluginManager.startUp( transactionManager, authenticator );

        // Add tracker, which rechecks constraints after enabling
        ConstraintTracker tracker = new ConstraintTracker( transactionManager );
        RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.addObserver( tracker );
        RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.addObserver( tracker );

        log.info( "****************************************************************************************************" );
        log.info( "                Polypheny-DB successfully started and ready to process your queries!" );
        log.info( "                              The UI is waiting for you on port {}:", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        log.info( "                                       http://localhost:{}", RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        log.info( "****************************************************************************************************" );
        isReady = true;

        // Close splash screen
        if ( showSplashScreen ) {
            splashScreen.setComplete();
        }

        try {
            log.trace( "Waiting for the Shutdown-Hook to finish ..." );
            sh.join( 0 ); // "forever"
            if ( !sh.hasFinished() ) {
                log.warn( "The Shutdown-Hook has not finished execution, but join() returned ..." );
            } else {
                log.info( "Waiting for the Shutdown-Hook to finish ... done." );
            }
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted while waiting for the Shutdown-Hook to finish. The JVM might terminate now without having terminate() on all components invoked.", e );
        }

        if ( trayMenu ) {
            TrayGui.getInstance().shutdown();
        }
    }


    public void loadDefaults() {
        Catalog.getInstance().restoreInterfacesIfNecessary();
    }

}
