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

package org.polypheny.db;

import com.github.rvesse.airline.HelpOption;
import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.parser.errors.ParseException;
import java.awt.SystemTray;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import javax.inject.Inject;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.StatusNotificationService.ErrorConfig;
import org.polypheny.db.StatusNotificationService.StatusType;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.impl.PolyCatalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.cli.PolyModesConverter;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigManager;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManagerImpl;
import org.polypheny.db.ddl.DefaultInserter;
import org.polypheny.db.docker.AutoDocker;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.gui.GuiUtils;
import org.polypheny.db.gui.SplashHelper;
import org.polypheny.db.gui.TrayGui;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.information.StatusService;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.information.HostInformation;
import org.polypheny.db.monitoring.information.JavaInformation;
import org.polypheny.db.monitoring.statistics.StatisticQueryProcessor;
import org.polypheny.db.monitoring.statistics.StatisticsManagerImpl;
import org.polypheny.db.partition.FrequencyMap;
import org.polypheny.db.partition.FrequencyMapImpl;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.PartitionManagerFactoryImpl;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.processing.AlgProcessor;
import org.polypheny.db.processing.AuthenticatorImpl;
import org.polypheny.db.processing.ConstraintEnforceAttacher.ConstraintTracker;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.TransactionManagerImpl;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.view.MaterializedViewManager;
import org.polypheny.db.view.MaterializedViewManagerImpl;
import org.polypheny.db.webui.ConfigService;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.InformationService;
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
    public static boolean memoryCatalog = false;

    @Option(name = { "-mode" }, description = "Special system configuration for running tests", typeConverterProvider = PolyModesConverter.class)
    public static RunMode mode = RunMode.PRODUCTION;

    @Option(name = { "-gui" }, description = "Show splash screen on startup and add taskbar gui")
    public boolean desktopMode = false;

    @Option(name = { "-daemon" }, description = "Disable splash screen")
    public boolean daemonMode = false;

    @Option(name = { "-defaultStore" }, description = "Type of default storeId")
    public String defaultStoreName = "hsqldb";

    @Option(name = { "-defaultSource" }, description = "Type of default source")
    public static String defaultSourceName = "csv";

    @Option(name = { "-c", "--config" }, description = "Path to the configuration file", type = OptionType.GLOBAL)
    protected String applicationConfPath;

    @Option(name = { "-v", "--version" }, description = "Current version of Polypheny-DB")
    public boolean versionOptionEnabled = false;

    // required for unit tests to determine when the system is ready to process queries
    @Getter
    private volatile boolean isReady = false;
    private SplashHelper splashScreen;


    public static void main( final String[] args ) {
        try {
            if ( log.isDebugEnabled() ) {
                log.debug( "PolyphenyDb.main( {} )", java.util.Arrays.toString( args ) );
            }
            TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
            final SingleCommand<PolyphenyDb> parser = SingleCommand.singleCommand( PolyphenyDb.class );
            final PolyphenyDb polyphenyDb = parser.parse( args );

            StatusNotificationService.addSubscriber( log::info, StatusType.INFO );
            StatusNotificationService.addSubscriber( log::error, StatusType.ERROR );

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
        } catch (
                ParseException e ) {
            log.error( "Error parsing command line parameters: " + e.getMessage() );
        } catch (
                Throwable uncaught ) {
            if ( log.isErrorEnabled() ) {
                log.error( "Uncaught Throwable.", uncaught );
                StatusNotificationService.printError(
                        "Error: " + uncaught.getMessage(),
                        ErrorConfig.builder().func( ErrorConfig.DO_NOTHING ).doExit( true ).showButton( true ).buttonMessage( "Exit" ).build() );
            }
        }
    }


    public void runPolyphenyDb() {
        if ( resetDocker ) {
            log.warn( "[-resetDocker] option is set, this option is only for development." );
        }

        // Configuration shall not be persisted
        ConfigManager.memoryMode = (mode == RunMode.TEST || memoryCatalog);
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

        // we have to set the mode before checking
        PolyphenyHomeDirManager dirManager = PolyphenyHomeDirManager.setModeAndGetInstance( mode );

        initializeStatusNotificationService();

        // Check if Polypheny is already running
        if ( GuiUtils.checkPolyphenyAlreadyRunning() ) {
            if ( openUiInBrowser ) {
                GuiUtils.openUiInBrowser();
            }
            System.err.println( "There is already an instance of Polypheny running on this system." );
            System.exit( 0 );
        }

        // Restore content of Polypheny folder
        restoreHomeFolderIfNecessary( dirManager );

        // Reset catalog, data and configuration
        if ( resetCatalog ) {
            if ( !PolyphenyHomeDirManager.getInstance().recursiveDeleteFolder( "data" ) ) {
                log.error( "Unable to delete the data folder." );
            }
            if ( !PolyphenyHomeDirManager.getInstance().recursiveDeleteFolder( "monitoring" ) ) {
                log.error( "Unable to delete the monitoring folder." );
            }
            ConfigManager.getInstance().resetDefaultConfiguration();
        }

        // Backup content of Polypheny folder
        if ( mode == RunMode.TEST || memoryCatalog ) {
            if ( dirManager.getHomeFile( "_test_backup" ).isPresent() ) {
                throw new GenericRuntimeException( "Unable to backup the Polypheny folder since there is already a backup folder." );
            }
            File backupFolder = dirManager.registerNewFolder( "_test_backup" );
            for ( File item : dirManager.getRootPath().listFiles() ) {
                if ( item.getName().equals( "_test_backup" ) ) {
                    continue;
                }
                if ( !item.renameTo( new File( backupFolder, item.getName() ) ) ) {
                    throw new GenericRuntimeException( "Unable to backup the Polypheny folder." );
                }
            }
            log.info( "Restoring the Polypheny folder." );
        }

        // Enables Polypheny to be started with a different config.
        // Otherwise, Config at default location is used.
        if ( applicationConfPath != null && PolyphenyHomeDirManager.getInstance().getHomeFile( applicationConfPath ).isPresent() ) {
            ConfigManager.getInstance();
            ConfigManager.setApplicationConfFile( new File( applicationConfPath ) );
        }

        // Generate UUID for Polypheny (if there isn't one already)
        String uuid;
        if ( PolyphenyHomeDirManager.getInstance().getGlobalFile( "uuid" ).isEmpty() ) {
            UUID id = UUID.randomUUID();
            File f = PolyphenyHomeDirManager.getInstance().registerNewGlobalFile( "uuid" );

            try ( FileOutputStream out = new FileOutputStream( f ) ) {
                out.write( id.toString().getBytes( StandardCharsets.UTF_8 ) );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Failed to store UUID " + e );
            }

            uuid = id.toString();
        } else {
            Path path = PolyphenyHomeDirManager.getInstance().getGlobalFile( "uuid" ).orElseThrow().toPath();

            try ( BufferedReader in = Files.newBufferedReader( path, StandardCharsets.UTF_8 ) ) {
                uuid = UUID.fromString( in.readLine() ).toString();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Failed to load UUID " + e );
            }
        }

        if ( mode == RunMode.TEST ) {
            uuid = "polypheny-test";
        }

        log.info( "Polypheny UUID: " + uuid );
        RuntimeConfig.INSTANCE_UUID.setString( uuid );

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

        // Start Polypheny-UI
        final Authenticator authenticator = new AuthenticatorImpl();
        final HttpServer server = startHttpServer( authenticator, transactionManager );

        // Start config server and information server
        new ConfigService( server.getServer() );
        new InformationService( server.getServer() );

        try {
            new JavaInformation();
        } catch ( Exception e ) {
            log.error( "Unable to retrieve java runtime information." );
        }
        try {
            HostInformation.getINSTANCE();
        } catch ( Exception e ) {
            log.error( "Unable to retrieve host information." );
        }

        // Status service which pipes msgs to the start ui or the console
        StatusService.initialize( transactionManager, server.getServer() );

        log.debug( "Setting Docker Timeouts" );
        Catalog.resetDocker = resetDocker;
        RuntimeConfig.DOCKER_TIMEOUT.setInteger( mode == RunMode.DEVELOPMENT || mode == RunMode.TEST ? 5 : RuntimeConfig.DOCKER_TIMEOUT.getInteger() );
        if ( initializeDockerManager() ) {
            return;
        }

        // Initialize plugin manager
        PolyPluginManager.init( resetPlugins );

        // Startup and restore catalog
        Catalog catalog = startCatalog();

        // Initialize interface manager
        QueryInterfaceManager.initialize( transactionManager, authenticator );

        // Call DockerManager once to remove old containers
        DockerManager.getInstance();

        // Initialize PartitionMangerFactory
        PartitionManagerFactory.setAndGetInstance( new PartitionManagerFactoryImpl() );
        FrequencyMap.setAndGetInstance( new FrequencyMapImpl( catalog ) );

        // temporary add sql and rel here
        QueryLanguage language = new QueryLanguage(
                DataModel.RELATIONAL,
                "alg",
                List.of( "alg", "algebra" ),
                null,
                AlgProcessor::new,
                null,
                q -> null,
                c -> c );
        LanguageManager.getINSTANCE().addQueryLanguage( language );

        // Initialize index manager
        initializeIndexManager();

        // Initialize statistic manager
        final StatisticQueryProcessor statisticQueryProcessor = new StatisticQueryProcessor( transactionManager, authenticator );
        StatisticsManager.setAndGetInstance( new StatisticsManagerImpl( statisticQueryProcessor ) );

        // Initialize MaterializedViewManager
        MaterializedViewManager.setAndGetInstance( new MaterializedViewManagerImpl( transactionManager ) );

        // Initialize DDL Manager
        DdlManager.setAndGetInstance( new DdlManagerImpl( catalog ) );

        // Add config and monitoring test page for UI testing
        if ( mode == RunMode.TEST ) {
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

        PolyPluginManager.initAfterCatalog();
        //noinspection ResultOfMethodCallIgnored
        RoutingManager.getInstance();

        PolyPluginManager.initAfterTransaction( transactionManager );

        restore( authenticator, catalog );

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

        // Initialize statistic settings
        StatisticsManager.getInstance().initializeStatisticSettings();

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


    private boolean initializeDockerManager() {
        if ( AutoDocker.getInstance().isAvailable() ) {
            if ( mode == RunMode.TEST ) {
                resetDocker = true;
                Catalog.resetDocker = true;
            }
            boolean success = AutoDocker.getInstance().doAutoConnect();
            if ( mode == RunMode.TEST && !success ) {
                // AutoDocker does not work in Windows containers
                if ( !System.getenv( "RUNNER_OS" ).equals( "Windows" ) ) {
                    log.error( "Failed to connect to docker instance" );
                    return true;
                }
            }
        }
        return false;
    }


    private static void initializeStatusNotificationService() {
        StatusNotificationService.setPort( RuntimeConfig.WEBUI_SERVER_PORT.getInteger() );
        RuntimeConfig.WEBUI_SERVER_PORT.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                StatusNotificationService.setPort( c.getInt() );
            }


            @Override
            public void restart( Config c ) {
                StatusNotificationService.setPort( c.getInt() );
            }
        } );
    }


    private void initializeIndexManager() {
        try {
            IndexManager.getInstance().initialize( transactionManager );
            IndexManager.getInstance().restoreIndexes();
        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( "Something went wrong while initializing index manager.", e );
        }
    }


    private static void restoreHomeFolderIfNecessary( PolyphenyHomeDirManager dirManager ) {
        if ( dirManager.getHomeFile( "_test_backup" ).isPresent() && dirManager.getHomeFile( "_test_backup" ).get().isDirectory() ) {
            File backupFolder = dirManager.getHomeFile( "_test_backup" ).get();
            // Cleanup Polypheny folder
            for ( File item : dirManager.getRootPath().listFiles() ) {
                if ( item.getName().equals( "_test_backup" ) ) {
                    continue;
                }
                if ( dirManager.getHomeFile( item.getName() ).orElseThrow().isFile() ) {
                    dirManager.deleteFile( item.getName() );
                } else {
                    dirManager.recursiveDeleteFolder( item.getName() );
                }
            }
            // Restore contents from backup
            for ( File item : backupFolder.listFiles() ) {
                if ( dirManager.getHomeFile( "_test_backup/" + item.getName() ).isPresent() ) {
                    if ( !item.renameTo( new File( dirManager.getRootPath(), item.getName() ) ) ) {
                        throw new GenericRuntimeException( "Unable to restore the Polypheny folder." );
                    }
                }
            }
            //noinspection ResultOfMethodCallIgnored
            backupFolder.delete();
            log.info( "Restoring the data folder." );
        }
    }


    private HttpServer startHttpServer( Authenticator authenticator, TransactionManager transactionManager ) {
        final HttpServer httpServer = new HttpServer( transactionManager, authenticator );
        Thread polyphenyUiThread = new Thread( httpServer );
        polyphenyUiThread.start();
        try {
            polyphenyUiThread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }
        return httpServer;
    }


    @NotNull
    private Catalog startCatalog() {
        Catalog.resetCatalog = resetCatalog;
        Catalog.memoryCatalog = memoryCatalog;
        Catalog.mode = mode;
        Catalog.resetDocker = resetDocker;

        Catalog catalog = Catalog.setAndGetInstance( new PolyCatalog() );

        if ( catalog == null ) {
            throw new GenericRuntimeException( "There was no catalog submitted, aborting." );
        }
        catalog.init();
        return catalog;
    }


    /**
     * Restores the previous state of Polypheny, which is provided by the catalog.
     *
     * @param authenticator the authenticator, which is not used atm
     * @param catalog the current catalog, which provides the last state
     */
    private void restore( Authenticator authenticator, Catalog catalog ) {
        PolyPluginManager.startUp( transactionManager, authenticator );

        Transaction trx = transactionManager.startTransaction(
                Catalog.defaultUserId,
                Catalog.defaultNamespaceId,
                false,
                "Catalog Startup" );
        if ( !resetCatalog && !memoryCatalog && mode != RunMode.TEST ) {
            Catalog.getInstance().restore( trx );
        }
        Catalog.getInstance().updateSnapshot();

        Catalog.defaultStore = AdapterTemplate.fromString( defaultStoreName, AdapterType.STORE );
        Catalog.defaultSource = AdapterTemplate.fromString( defaultSourceName, AdapterType.SOURCE );
        restoreDefaults( catalog, mode );

        QueryInterfaceManager.getInstance().restoreInterfaces( catalog.getSnapshot() );

        commitRestore( trx );
    }


    /**
     * Tries to commit the restored catalog.
     */
    private void commitRestore( Transaction trx ) {
        try {
            trx.commit();
        } catch ( TransactionException e ) {
            try {
                trx.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Error while rolling back the transaction", e );
            }
            throw new GenericRuntimeException( "Something went wrong while restoring stores from the catalog.", e );
        }
    }


    /**
     * Restores the default structure, interfaces, adapters.
     *
     * @param catalog the current catalog
     * @param mode the current mode
     */
    private static void restoreDefaults( Catalog catalog, RunMode mode ) {
        catalog.updateSnapshot();
        DefaultInserter.resetData( DdlManager.getInstance(), mode );
        DefaultInserter.restoreInterfacesIfNecessary();
    }


}
