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

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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
import org.pf4j.PluginWrapper;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.ConfigPlugin;
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


    public static void init() {
        pluginManager.loadPlugins();
        pluginManager.startPlugins();

        RuntimeConfig.AVAILABLE_PLUGINS.setList(
                pluginManager.getStartedPlugins().stream().map( p -> (PolyPluginDescriptor) p.getDescriptor() )
                        .map( p -> new ConfigPlugin( p.getPluginId(), org.polypheny.db.config.PluginStatus.ACTIVE,
                                p.imagePath, p.categories, p.getPluginDescription(), p.getVersion(), p.isSystemComponent, p.isUiVisible ) )
                        .toList()
        );

        PLUGINS.putAll( pluginManager.getStartedPlugins().stream().collect( Collectors.toMap( PluginWrapper::getPluginId, p -> p ) ) );

        // print extensions for each started plugin
        for ( PluginWrapper plugin : pluginManager.getStartedPlugins() ) {
            String pluginId = plugin.getDescriptor().getPluginId();
            log.info( "Plugin '{}' added", pluginId );
        }
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
                throw new GenericRuntimeException( "Plugin is missing required key: %s", key );
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


    public record PluginStatus( @JsonSerialize String id, @JsonSerialize boolean loaded, @JsonSerialize String path, @JsonSerialize String imagePath, @JsonSerialize boolean isSystemComponent, @JsonSerialize boolean isUiVisible ) {

        public static PluginStatus from( PluginWrapper wrapper ) {
            PolyPluginDescriptor descriptor = ((PolyPluginDescriptor) wrapper.getDescriptor());
            return new PluginStatus( wrapper.getPluginId(), PLUGINS.containsKey( wrapper.getPluginId() ), wrapper.getPluginPath().toAbsolutePath().toString(), descriptor.getImagePath(), descriptor.isSystemComponent, descriptor.isUiVisible );
        }

    }

}
