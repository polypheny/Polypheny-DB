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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ClassLoadingStrategy;
import org.pf4j.CompoundPluginDescriptorFinder;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginLoader;
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;

@Slf4j
public class PolyPluginManager extends DefaultPluginManager {

    private static PluginState state = PluginState.DIRTY;

    @Getter
    private static PersistentMonitoringRepository PERSISTENT_MONITORING;

    @Getter
    private static Supplier<Catalog> CATALOG_SUPPLIER;

    @Getter
    public static List<PluginWrapper> PLUGINS = new ArrayList<>();

    public static List<Runnable> AFTER_INIT = new ArrayList<>();

    @Getter
    private static PluginClassLoader mainClassLoader;
    // create the plugin manager
    private static final PolyPluginManager pluginManager = new PolyPluginManager( "../build/plugins", "./build/plugins", "../../build/plugins" );


    public PolyPluginManager( String... paths ) {
        super( Arrays.stream( paths ).map( Path::of ).collect( Collectors.toList() ) );
    }


    public static void init() {

        // load the plugins
        pluginManager.loadPlugins();

        // start (active/resolved) the plugins
        pluginManager.startPlugins();

        PLUGINS.addAll( pluginManager.getStartedPlugins() );

        // print extensions for each started plugin
        List<PluginWrapper> startedPlugins = pluginManager.getStartedPlugins();
        for ( PluginWrapper plugin : startedPlugins ) {
            String pluginId = plugin.getDescriptor().getPluginId();

            log.info( String.format( "Plugin '%s' added", pluginId ) );
        }
        // reset the state
        state = PluginState.CLEAN;
    }


    public static PluginStatus loadAdditionalPlugin( String path ) {
        AFTER_INIT.clear();
        PluginStatus status = new PluginStatus( path );
        String pluginId;
        try {
            pluginId = pluginManager.loadPlugin( status.path );
        } catch ( Exception e ) {
            return status.loaded( false );
        }
        PLUGINS.add( pluginManager.getStartedPlugins().stream().filter( p -> p.getPluginId().equals( pluginId ) ).findFirst().orElseThrow() );
        AFTER_INIT.forEach( Runnable::run );

        return status.loaded( true );
    }


    public static PluginStatus unloadAdditionalPlugin( String pluginId ) {
        PluginStatus status = new PluginStatus( pluginManager.getStartedPlugins().stream().filter( p -> p.getPluginId().equals( pluginId ) ).findFirst().orElseThrow().getPluginId() );
        try {
            pluginManager.unloadPlugin( pluginId );
        } catch ( Exception e ) {
            return status.loaded( true );
        }
        PLUGINS.remove( pluginId );
        state = PluginState.DIRTY;
        return status.loaded( false );
    }


    public static void startUp() {
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
                .add( new ManifestPluginDescriptorFinder() );
    }


    private static class PluginStatus {

        @Accessors(fluent = true)
        @Setter
        boolean loaded = false;
        final String stringPath;
        final Path path;


        public PluginStatus( String stringPath ) {
            this.stringPath = stringPath;
            this.path = Path.of( stringPath );
        }

    }

}
