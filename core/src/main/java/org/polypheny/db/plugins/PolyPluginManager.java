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

package org.polypheny.db.plugins;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.commons.io.FileUtils;
import org.pf4j.ClassLoadingStrategy;
import org.pf4j.CompoundPluginDescriptorFinder;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginDescriptor;
import org.pf4j.DefaultPluginFactory;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.JarPluginLoader;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.Plugin;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginFactory;
import org.pf4j.PluginLoader;
import org.pf4j.PluginState;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigPlugin;
import org.polypheny.db.config.ConfigString;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

/**
 * Own implementation of the PluginManager from PF4J, which handles the default location, where plugins are loaded from.
 * Custom properties include picture.
 */
@Slf4j
public class PolyPluginManager extends DefaultPluginManager {

    @Getter
    private final static AtomicReference<PersistentMonitoringRepository> PERSISTENT_MONITORING = new AtomicReference<>( null );

    @Getter
    public static ObservableMap<String, PluginWrapper> PLUGINS = new ObservableMap<>();

    public static List<Runnable> AFTER_INIT = new ArrayList<>();

    @Getter
    private static PluginClassLoader mainClassLoader;
    // create the plugin manager
    private static final PolyPluginManager pluginManager;


    static {
        final File jarFile = new File( PolyPluginManager.class.getProtectionDomain().getCodeSource().getLocation().getPath() );
        File pluginsFolder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "plugins" );
        if ( jarFile.isFile() ) {  // Run with JAR file
            // Copy plugins bundled into the jar file to the designated plugins' folder.
            // Overwrites existing plugins with same name (name includes version number).
            try ( JarFile jar = new JarFile( jarFile ) ) {
                for ( String name : jar.stream().map( JarEntry::getName ).toList() ) {
                    if ( name.startsWith( "plugins/" ) && name.endsWith( ".zip" ) ) {
                        FileUtils.copyURLToFile(
                                PolyPluginManager.class.getResource( "/" + name ),
                                new File( pluginsFolder, name.split( "/" )[1] ) );
                    }
                }
            } catch ( Exception e ) {
                // ignore
            }
        }
        pluginManager = new PolyPluginManager(
                pluginsFolder.toPath(),
                Path.of( "../build/plugins" ),
                Path.of( "./build/plugins" ),
                Path.of( "../../build/plugins" ) );
    }


    public PolyPluginManager( Path... paths ) {
        super( paths );
    }


    public static void initAfterCatalog() {
        getPLUGINS().values().forEach( p -> ((PolyPlugin) p.getPlugin()).afterCatalogInit() );
    }


    public static void initAfterTransaction( TransactionManager manager ) {
        getPLUGINS().values().forEach( p -> ((PolyPlugin) p.getPlugin()).afterTransactionInit( manager ) );
    }


    @Override
    protected PluginFactory createPluginFactory() {
        return new DefaultPluginFactory() {
            @Override
            protected Plugin createInstance( Class<?> pluginClass, PluginWrapper pluginWrapper ) {
                PluginContext context = new PluginContext( pluginWrapper.getRuntimeMode() );
                try {
                    Constructor<?> constructor = pluginClass.getConstructor( PluginContext.class );
                    return (Plugin) constructor.newInstance( context );
                } catch ( Exception e ) {
                    log.error( e.getMessage(), e );
                }

                return null;
            }
        };
    }


    public static void init( boolean resetPluginsOnStartup ) {
        if ( resetPluginsOnStartup ) {
            RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class ).clear();
            RuntimeConfig.BLOCKED_PLUGINS.getList( ConfigString.class ).clear();
        }

        attachRuntimeToPlugins();

        pluginManager.loadPlugins();

        // start (active/resolved) the plugins
        if ( RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class ).isEmpty() ) {
            // no old config there so we just start, except the blocked ones
            for ( PluginWrapper resolvedPlugin : pluginManager.resolvedPlugins ) {
                if ( !RuntimeConfig.BLOCKED_PLUGINS.getStringList().contains( resolvedPlugin.getPluginId() ) ) {
                    pluginManager.startPlugin( resolvedPlugin.getPluginId() );
                }
            }
            pluginManager.startPlugins();
        } else {
            for ( ConfigPlugin plugin : RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class ) ) {
                if ( plugin.getStatus() == org.polypheny.db.config.PluginStatus.ACTIVE ) {
                    pluginManager.startPlugin( plugin.getPluginId() );
                }
            }
        }

        PLUGINS.putAll( pluginManager.getStartedPlugins().stream().collect( Collectors.toMap( PluginWrapper::getPluginId, p -> p ) ) );

        attachPluginsToRuntime();

        // print extensions for each started plugin
        List<PluginWrapper> startedPlugins = pluginManager.getStartedPlugins();
        for ( PluginWrapper plugin : startedPlugins ) {
            String pluginId = plugin.getDescriptor().getPluginId();
            log.info( "Plugin '{}' added", pluginId );
        }
    }


    private static void attachRuntimeToPlugins() {
        PLUGINS.addListener( e -> RuntimeConfig.AVAILABLE_PLUGINS.setList(
                PLUGINS
                        .values()
                        .stream()
                        .map( p -> (PolyPluginDescriptor) p.getDescriptor() )
                        .map( d -> new ConfigPlugin(
                                d.getPluginId(),
                                org.polypheny.db.config.PluginStatus.ACTIVE,
                                d.imagePath, d.categories,
                                d.getPluginDescription(),
                                d.getVersion(),
                                d.isSystemComponent,
                                d.isUiVisible ) )
                        .collect( Collectors.toList() ) ) );
    }


    private static void attachPluginsToRuntime() {
        RuntimeConfig.AVAILABLE_PLUGINS.addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                // check if still the same plugins are present
                List<ConfigPlugin> configs = RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class );

                List<String> removed = configs.stream().map( ConfigPlugin::getPluginId ).collect( Collectors.toList() );
                PLUGINS.keySet().forEach( removed::remove );

                configs.forEach( p -> {
                    if ( toState( p.getStatus() ) != PLUGINS.get( p.getPluginId() ).getPluginState() ) {
                        if ( p.getStatus() == org.polypheny.db.config.PluginStatus.ACTIVE ) {
                            // start
                            startAvailablePlugin( p.getPluginId() );
                        } else {
                            // stop
                            stopAvailablePlugin( p.getPluginId() );
                        }
                    }
                } );

            }


            @Override
            public void restart( Config c ) {

            }
        } );
    }


    /**
     * Stop a plugin, which was loaded and started previously.
     *
     * @param pluginId identifier of the plugin
     */
    private static void stopAvailablePlugin( String pluginId ) {
        if ( !PLUGINS.containsKey( pluginId ) ) {
            throw new GenericRuntimeException( "Plugin is not not loaded and can not be stopped." );
        }
        PluginWrapper plugin = PLUGINS.get( pluginId );

        if ( plugin.getPluginState() != PluginState.STARTED ) {
            throw new GenericRuntimeException( "Plugin is not active and can not be stopped." );
        }
        PolyPluginDescriptor descriptor = (PolyPluginDescriptor) plugin.getDescriptor();
        if ( descriptor.isSystemComponent ) {
            throw new GenericRuntimeException( "Plugin is system component and cannot be stopped." );
        }

        pluginManager.stopPlugin( pluginId );
        PLUGINS.get( pluginId ).setPluginState( org.pf4j.PluginState.STOPPED );
    }


    /**
     * Starts a plugin, which was loaded previously.
     *
     * @param pluginId identifier of the plugin
     */
    private static void startAvailablePlugin( String pluginId ) {
        if ( !PLUGINS.containsKey( pluginId ) ) {
            throw new GenericRuntimeException( "Plugin is not not loaded and can not be started." );
        }

        PolyPluginDescriptor descriptor = (PolyPluginDescriptor) PLUGINS.get( pluginId ).getDescriptor();

        if ( descriptor.hasVersionDependencies ) {
            log.debug( "Cannot load plugin {} because version dependencies are not yet supported", pluginId );
            return;
        }

        pluginManager.startPlugin( pluginId );
        PLUGINS.get( pluginId ).setPluginState( org.pf4j.PluginState.STARTED );
    }


    /**
     * Mapping of the internal PluginStatus to the PF4J PluginStatus
     *
     * @param status the status in the Polypheny format
     * @return the status in the PF4j format
     */
    private static org.pf4j.PluginState toState( org.polypheny.db.config.PluginStatus status ) {
        return switch ( status ) {
            case UNLOADED -> PluginState.CREATED;
            case LOADED -> PluginState.DISABLED;
            case ACTIVE -> PluginState.STARTED;
        };
    }


    /**
     * The processes, which plugins can register on start, which will be executed as late as possible.
     *
     * @param transactionManager the transactionManager, which plugins like explore-by-example use
     * @param authenticator the authenticator, which plugins like mapdb-catalog use
     */
    public static void startUp( TransactionManager transactionManager, Authenticator authenticator ) {
        // hand parameters to extensions
        TransactionExtension.REGISTER.forEach( e -> e.initExtension( transactionManager, authenticator ) );

        AFTER_INIT.forEach( Runnable::run );
    }


    /**
     * Allows to set a {@link PersistentMonitoringRepository } used to store monitoring events during runtime.
     *
     * @param repository the implementation
     */
    public static void setPersistentRepository( PersistentMonitoringRepository repository ) {
        if ( !PERSISTENT_MONITORING.compareAndSet( null, repository ) ) {
            throw new GenericRuntimeException( "There is already a persistent repository." );
        }
    }


    /**
     * Creates a single {@link ClassLoader}, which holds the classes for all loaded plugins.
     *
     * We load the existing applications classes first, then the dependencies and then the plugin
     * We have to reuse the classloader else the code generation will not be able to find the added classes later on
     *
     * @return the custom {@link ClassLoader}
     */
    @Override
    protected PluginLoader createPluginLoader() {
        return new CompoundPluginLoader()
                .add( new DefaultPluginLoader( this ) {
                    @Override
                    protected PluginClassLoader createPluginClassLoader( Path pluginPath, PluginDescriptor pluginDescriptor ) {
                        return getCustomClassLoader( pluginDescriptor );
                    }
                } )
                .add( new JarPluginLoader( this ) {
                    @Override
                    public ClassLoader loadPlugin( Path pluginPath, PluginDescriptor pluginDescriptor ) {
                        return getCustomClassLoader( pluginDescriptor );
                    }
                } );
    }


    public static PluginClassLoader getCustomClassLoader( PluginDescriptor pluginDescriptor ) {
        if ( mainClassLoader == null ) {
            //mainClassLoader = new URLClassLoader( new URL[0], PolyPluginManager.class.getClassLoader() );
            mainClassLoader = new PluginClassLoader( pluginManager, pluginDescriptor, PolyPluginManager.class.getClassLoader(), ClassLoadingStrategy.APD );
        }
        return mainClassLoader;
    }


    @Override
    protected CompoundPluginDescriptorFinder createPluginDescriptorFinder() {
        return new CompoundPluginDescriptorFinder()
                .add( new ManifestPluginDescriptorFinder() {

                    @Override
                    protected PluginDescriptor createPluginDescriptor( Manifest manifest ) {
                        return new PolyPluginDescriptor( super.createPluginDescriptor( manifest ), manifest );
                    }
                } );
    }


    /**
     * Custom plugin descriptor, which are unique and required for this implementation and the supported plugins.
     */
    public static class PolyPluginDescriptor extends DefaultPluginDescriptor {

        public static final String PLUGIN_ICON_PATH = "Plugin-Icon-Path";

        public static final String PLUGIN_CATEGORIES = "Plugin-Categories";

        public static final String PLUGIN_POLYPHENY_DEPENDENCIES = "Plugin-Polypheny-Dependencies";

        public static final String PLUGIN_SYSTEM_COMPONENT = "Plugin-System-Component";

        public static final String PLUGIN_UI_VISIBLE = "Plugin-Ui-Visible";

        @Getter
        private final String imagePath;

        @Getter
        private final List<String> categories;
        @Getter
        private final boolean hasVersionDependencies;
        private final boolean isSystemComponent;
        private final boolean isUiVisible;


        public PolyPluginDescriptor( PluginDescriptor descriptor, Manifest manifest ) {
            super( descriptor.getPluginId(), descriptor.getPluginDescription(), descriptor.getPluginClass(), descriptor.getVersion(), descriptor.getRequires(), descriptor.getProvider(), descriptor.getLicense() );
            this.imagePath = manifest.getMainAttributes().getValue( PLUGIN_ICON_PATH );
            this.categories = getCategories( manifest );
            this.hasVersionDependencies = hasVersionDependencies( manifest );
            this.isSystemComponent = Boolean.TRUE.equals( getManifestValue( Boolean::valueOf, manifest, PLUGIN_SYSTEM_COMPONENT ) );
            this.isUiVisible = Boolean.TRUE.equals( getManifestValue( Boolean::valueOf, manifest, PLUGIN_UI_VISIBLE ) );
        }


        private <T> T getManifestValue( Function1<String, T> transformer, Manifest manifest, String key ) {
            String attribute = manifest.getMainAttributes().getValue( key );

            if ( attribute == null ) {
                throw new GenericRuntimeException( "Plugin contains not all required keys: %s", key );
            }

            return transformer.apply( attribute );
        }


        private boolean hasVersionDependencies( Manifest manifest ) {
            String dep = manifest.getMainAttributes().getValue( PLUGIN_POLYPHENY_DEPENDENCIES );
            return dep != null && !dep.trim().isEmpty();
        }


        private List<String> getCategories( Manifest manifest ) {
            String categories = manifest.getMainAttributes().getValue( PLUGIN_CATEGORIES );

            if ( categories == null || categories.trim().isEmpty() ) {
                return List.of();
            }

            return Arrays.stream( categories.split( "," ) ).map( String::trim ).toList();
        }

    }


    public record PluginStatus( String id, boolean loaded, String path, String imagePath, boolean isSystemComponent, boolean isUiVisible ) {

        public static PluginStatus from( PluginWrapper wrapper ) {
            PolyPluginDescriptor descriptor = ((PolyPluginDescriptor) wrapper.getDescriptor());
            return new PluginStatus( wrapper.getPluginId(), PLUGINS.containsKey( wrapper.getPluginId() ), wrapper.getPluginPath().toAbsolutePath().toString(), descriptor.getImagePath(), descriptor.isSystemComponent, descriptor.isUiVisible );
        }

    }

}
