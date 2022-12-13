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

package org.polypheny.db.plugins;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ClassLoadingStrategy;
import org.pf4j.CompoundPluginDescriptorFinder;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginDescriptor;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginLoader;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigPlugin;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class PolyPluginManager extends DefaultPluginManager {

    @Getter
    private static PersistentMonitoringRepository PERSISTENT_MONITORING;

    @Getter
    private static Supplier<Catalog> CATALOG_SUPPLIER;

    @Getter
    public static ObservableMap<String, PluginWrapper> PLUGINS = new ObservableMap<>();

    public static List<Runnable> AFTER_INIT = new ArrayList<>();

    @Getter
    private static PluginClassLoader mainClassLoader;
    // create the plugin manager
    private static final PolyPluginManager pluginManager;


    static {
        pluginManager = new PolyPluginManager(
                "../build/plugins",
                "./build/plugins",
                "../../build/plugins",
                PolyphenyHomeDirManager.getInstance().registerNewFolder( "plugins" ).getPath() );
    }


    public PolyPluginManager( String... paths ) {
        super( Arrays.stream( paths ).map( Path::of ).collect( Collectors.toList() ) );
    }


    public static void init() {
        attachRuntimeToPlugins();

        // load the plugins
        pluginManager.loadPlugins();

        // start (active/resolved) the plugins
        if ( RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class ).isEmpty() ) {
            // no old config there so we just start all
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

            log.info( String.format( "Plugin '%s' added", pluginId ) );
        }
    }


    private static void attachRuntimeToPlugins() {
        PLUGINS.addListener( ( e ) -> {
            RuntimeConfig.AVAILABLE_PLUGINS.getList( ConfigPlugin.class ).clear();
            RuntimeConfig.AVAILABLE_PLUGINS.setList(
                    PLUGINS
                            .values()
                            .stream()
                            .map( p -> (PolyPluginDescriptor) p.getDescriptor() )
                            .map( d -> new ConfigPlugin( d.getPluginId(), org.polypheny.db.config.PluginStatus.ACTIVE, d.imagePath, d.categories, d.getPluginDescription(), d.getVersion() ) )
                            .collect( Collectors.toList() ) );
        } );
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


    private static void stopAvailablePlugin( String pluginId ) {
        pluginManager.stopPlugin( pluginId );
        PLUGINS.get( pluginId ).setPluginState( org.pf4j.PluginState.STOPPED );
    }


    private static void startAvailablePlugin( String pluginId ) {
        boolean isCompatible = isCompatible( ((PolyPluginDescriptor) PLUGINS.get( pluginId ).getDescriptor()).versionDependencies );

        if ( !isCompatible ) {
            log.debug( "Cannot load plugin {} with version {}.", pluginId, ((PolyPluginDescriptor) PLUGINS.get( pluginId ).getDescriptor()).versionDependencies );
            return;
        }

        pluginManager.startPlugin( pluginId );
        PLUGINS.get( pluginId ).setPluginState( org.pf4j.PluginState.STARTED );
    }


    private static boolean isCompatible( VersionDependency versionDependencies ) {
        // todo check if main version fits
        if ( versionDependencies.type == DependencyType.NONE ) {
            return true;
        }

        throw new RuntimeException( "Polypheny dependencies for plugins are not yet supported." );

    }


    private static org.pf4j.PluginState toState( org.polypheny.db.config.PluginStatus status ) {
        switch ( status ) {

            case UNLOADED:
                return org.pf4j.PluginState.CREATED;
            case LOADED:
                return org.pf4j.PluginState.DISABLED;
            case ACTIVE:
                return org.pf4j.PluginState.STARTED;
            default:
                throw new RuntimeException( "Could not find the corresponding plugin state." );
        }
    }


    public static PluginStatus loadAdditionalPlugin( File file ) {
        AFTER_INIT.clear();
        PluginStatus status = new PluginStatus( file.getPath(), null );
        PluginWrapper plugin;
        try {
            plugin = pluginManager.getPlugin( pluginManager.loadPlugin( file.toPath() ) );
            status.id( plugin.getPluginId() );
        } catch ( Exception e ) {
            return status.loaded( false );
        }
        PLUGINS.put( plugin.getPluginId(), plugin );
        AFTER_INIT.forEach( Runnable::run );

        return status.loaded( true ).imagePath( ((PolyPluginDescriptor) plugin.getDescriptor()).imagePath );
    }


    public static PluginStatus unloadAdditionalPlugin( String pluginId ) {
        PluginWrapper plugin = pluginManager.getStartedPlugins().stream().filter( p -> p.getPluginId().equals( pluginId ) ).findFirst().orElseThrow();
        PluginStatus status = new PluginStatus( plugin.getPluginId(), ((PolyPluginDescriptor) plugin.getDescriptor()).getImagePath() );
        try {
            pluginManager.unloadPlugin( pluginId );
        } catch ( Exception e ) {
            return status.loaded( true );
        }
        PLUGINS.remove( pluginId );
        return status.loaded( false );
    }


    public static void startUp( TransactionManager transactionManager, Authenticator authenticator ) {
        // hand parameters to extensions
        TransactionExtension.REGISTER.forEach( e -> e.initExtension( transactionManager, authenticator ) );

        AFTER_INIT.forEach( Runnable::run );
    }


    public static void setCatalogsSupplier( Supplier<Catalog> catalogSupplier ) {
        if ( CATALOG_SUPPLIER != null ) {
            throw new RuntimeException( "There is already a catalog supplier set." );
        }
        CATALOG_SUPPLIER = catalogSupplier;
    }


    public static void setPersistentRepository( PersistentMonitoringRepository repository ) {
        if ( PERSISTENT_MONITORING != null ) {
            throw new RuntimeException( "There is already a persistent repository." );
        }
        PERSISTENT_MONITORING = repository;
    }


    @Override
    protected PluginLoader createPluginLoader() {
        return new CompoundPluginLoader()
                .add( new DefaultPluginLoader( this ) {
                    @Override
                    protected PluginClassLoader createPluginClassLoader( Path pluginPath, PluginDescriptor pluginDescriptor ) {
                        // we load the existing applications classes first, then the dependencies and then the plugin
                        // we have to reuse the classloader else the code generation will not be able to find the added classes later on
                        if ( mainClassLoader == null ) {
                            mainClassLoader = new PluginClassLoader( pluginManager, pluginDescriptor, super.getClass().getClassLoader(), ClassLoadingStrategy.APD );
                        }
                        return mainClassLoader;
                    }


                } )
                /*.add( new JarPluginLoader( this ) )*/;
    }


    @Override
    protected CompoundPluginDescriptorFinder createPluginDescriptorFinder() {
        return new CompoundPluginDescriptorFinder()
                // Demo is using the Manifest file
                // PropertiesPluginDescriptorFinder is commented out just to avoid error log
                //.add( new PropertiesPluginDescriptorFinder() );
                .add( new ManifestPluginDescriptorFinder() {

                    @Override
                    protected PluginDescriptor createPluginDescriptor( Manifest manifest ) {
                        return new PolyPluginDescriptor( super.createPluginDescriptor( manifest ), manifest );
                    }
                } );
    }


    public static class PolyPluginDescriptor extends DefaultPluginDescriptor {

        public static final String PLUGIN_ICON_PATH = "Plugin-Icon-Path";

        public static final String PLUGIN_CATEGORIES = "Plugin-Categories";

        public static final String PLUGIN_POLYPHENY_DEPENDENCIES = "Plugin-Polypheny-Dependencies";

        @Getter
        private final String imagePath;

        @Getter
        private final List<String> categories;
        @Getter
        private final VersionDependency versionDependencies;


        public PolyPluginDescriptor( PluginDescriptor descriptor, Manifest manifest ) {
            super( descriptor.getPluginId(), descriptor.getPluginDescription(), descriptor.getPluginClass(), descriptor.getVersion(), descriptor.getRequires(), descriptor.getProvider(), descriptor.getLicense() );
            this.imagePath = manifest.getMainAttributes().getValue( PLUGIN_ICON_PATH );
            this.categories = getCategories( manifest );
            this.versionDependencies = getVersionDependencies( manifest );
        }


        private VersionDependency getVersionDependencies( Manifest manifest ) {
            String dep = manifest.getMainAttributes().getValue( PLUGIN_POLYPHENY_DEPENDENCIES );

            if ( dep == null || dep.trim().equals( "" ) ) {
                return new VersionDependency( DependencyType.NONE, null );
            }

            String[] splits = dep.split( "-" );

            if ( splits.length == 2 ) {
                return new VersionDependency( DependencyType.RANGE, Stream.of( splits ).map( String::trim ).collect( Collectors.toList() ) );
            }
            splits = dep.split( "," );

            if ( splits.length > 1 ) {
                return new VersionDependency( DependencyType.LIST, Stream.of( splits ).map( String::trim ).collect( Collectors.toList() ) );
            }

            return new VersionDependency( DependencyType.SINGLE, List.of( dep.trim() ) );

        }


        private List<String> getCategories( Manifest manifest ) {
            String categories = manifest.getMainAttributes().getValue( PLUGIN_CATEGORIES );

            if ( categories == null || categories.trim().equals( "" ) ) {
                return List.of();
            }

            return Arrays.stream( categories.split( "," ) ).map( String::trim ).collect( Collectors.toList() );
        }

    }


    @Accessors(fluent = true)
    public static class PluginStatus {


        @Setter
        boolean loaded = false;
        final String stringPath;

        @Setter
        private String id;

        @Setter
        private String imagePath;


        public PluginStatus( String stringPath, String imagePath ) {
            this.stringPath = stringPath;
            this.imagePath = imagePath;
        }


        public Path getPath() {
            return Path.of( stringPath );
        }


        public static PluginStatus from( PluginWrapper wrapper ) {
            return new PluginStatus( wrapper.getPluginPath().toAbsolutePath().toString(), ((PolyPluginDescriptor) wrapper.getDescriptor()).getImagePath() )
                    .id( wrapper.getPluginId() )
                    .loaded( PLUGINS.containsKey( wrapper.getPluginId() ) );
        }


        public static TypeAdapter<PluginStatus> getSerializer() {
            return new TypeAdapter<>() {
                @Override
                public void write( JsonWriter out, PluginStatus value ) throws IOException {
                    out.beginObject();
                    out.name( "id" );
                    out.value( value.id );
                    out.name( "stringPath" );
                    out.value( value.stringPath );
                    out.name( "loaded" );
                    out.value( value.loaded );
                    out.name( "imagePath" );
                    out.value( value.imagePath );
                    out.endObject();
                }


                @Override
                public PluginStatus read( JsonReader in ) throws IOException {
                    in.beginObject();
                    String id = null;
                    String stringPath = null;
                    boolean loaded = false;
                    String imagePath = null;

                    while ( in.peek() != JsonToken.END_OBJECT ) {
                        String name = in.nextName();
                        switch ( name ) {
                            case "id":
                                id = in.nextString();
                                break;
                            case "stringPath":
                                stringPath = in.nextString();
                                break;
                            case "loaded":
                                loaded = in.nextBoolean();
                                break;
                            case "imagePath":
                                imagePath = in.nextString();
                                break;
                            default:
                                log.error( "Name is not known." );
                                return null;
                        }
                    }
                    in.endObject();

                    return new PluginStatus( stringPath, imagePath ).id( id ).loaded( loaded );
                }
            };
        }

    }


    public static class ObservableMap<K, V> extends HashMap<K, V> {

        protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );


        public void addListener( PropertyChangeListener listener ) {
            this.listeners.addPropertyChangeListener( listener );
        }


        public void removeListener( PropertyChangeListener listener ) {
            this.listeners.removePropertyChangeListener( listener );
        }


        @Override
        public V put( K key, V value ) {
            V val = super.put( key, value );
            listeners.firePropertyChange( new PropertyChangeEvent( this, "put", null, key ) );
            return val;
        }


        @Override
        public void clear() {
            super.clear();
            listeners.firePropertyChange( new PropertyChangeEvent( this, "clear", null, null ) );
        }


        @Override
        public void putAll( Map<? extends K, ? extends V> m ) {
            super.putAll( m );
            listeners.firePropertyChange( new PropertyChangeEvent( this, "putAll", null, m ) );
        }

    }


    public enum DependencyType {
        SINGLE,
        RANGE,
        LIST,
        NONE;
    }


    @AllArgsConstructor
    public static class VersionDependency {

        public DependencyType type;

        final List<String> versions;

    }

}
