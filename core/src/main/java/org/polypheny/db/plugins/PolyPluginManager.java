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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ClassLoadingStrategy;
import org.pf4j.CompoundPluginDescriptorFinder;
import org.pf4j.CompoundPluginLoader;
import org.pf4j.DefaultPluginLoader;
import org.pf4j.DefaultPluginManager;
import org.pf4j.JarPluginLoader;
import org.pf4j.ManifestPluginDescriptorFinder;
import org.pf4j.PluginClassLoader;
import org.pf4j.PluginDescriptor;
import org.pf4j.PluginLoader;
import org.pf4j.PluginManager;
import org.pf4j.PluginWrapper;

@Slf4j
public class PolyPluginManager extends DefaultPluginManager {

    public static List<String> REGISTER = new ArrayList<>();

    public static List<Runnable> AFTER_INIT = new ArrayList<>();


    public PolyPluginManager( String... paths ) {
        super( Arrays.stream( paths ).map( Path::of ).collect( Collectors.toList() ) );
    }


    public static void init() {
        // create the plugin manager
        final PluginManager pluginManager = new PolyPluginManager( "../build/plugins", "./build/plugins" );

        // load the plugins
        pluginManager.loadPlugins();

        // enable a disabled plugin
        // pluginManager.disablePlugin("explore-by-example");

        // start (active/resolved) the plugins
        pluginManager.startPlugins();

        log.info( "Plugin Directory: " );
        log.info( "\t" + System.getProperty( "pf4j.pluginsDir", "plugins" ) + "\n" );

        REGISTER.addAll( pluginManager.getStartedPlugins().stream().map( PluginWrapper::getPluginId ).collect( Collectors.toList() ) );

        // print extensions for each started plugin
        List<PluginWrapper> startedPlugins = pluginManager.getStartedPlugins();
        for ( PluginWrapper plugin : startedPlugins ) {
            String pluginId = plugin.getDescriptor().getPluginId();

            log.info( String.format( "Extensions added by plugin '%s':", pluginId ) );
            // pluginManager.getExtensionClassNames( pluginId ).forEach( e -> log.info( "\t" + e ) ); // takes forever
        }
        // List<TransactionExtension> exceptions = pluginManager.getExtensions( TransactionExtension.class ); // does not work with ADP

    }


    public static void startUp() {
        AFTER_INIT.forEach( Runnable::run );
    }


    public static PluginClassLoader loader;


    @Override
    protected PluginLoader createPluginLoader() {
        return new CompoundPluginLoader()
                .add( new DefaultPluginLoader( this ) {
                    @Override
                    protected PluginClassLoader createPluginClassLoader( Path pluginPath, PluginDescriptor pluginDescriptor ) {
                        // we load the existing applications classes first, then the dependencies and then the plugin
                        // we have to reuse the classloader else the code generation will not be able to find the added classes later on
                        //return new PluginClassLoader( pluginManager, pluginDescriptor, super.getClass().getClassLoader(), ClassLoadingStrategy.ADP );
                        if ( loader == null ) {
                            loader = new PluginClassLoader( pluginManager, pluginDescriptor, super.getClass().getClassLoader(), ClassLoadingStrategy.ADP );
                        }
                        return loader;
                    }


                    @Override
                    public ClassLoader loadPlugin( Path pluginPath, PluginDescriptor pluginDescriptor ) {
                        PluginClassLoader pluginClassLoader = createPluginClassLoader( pluginPath, pluginDescriptor );

                        loadClasses( pluginPath, pluginClassLoader );
                        loadJars( pluginPath, pluginClassLoader );

                        return pluginClassLoader;
                    }
                } )
                .add( new JarPluginLoader( this ) );
    }


    @Override
    protected CompoundPluginDescriptorFinder createPluginDescriptorFinder() {
        return new CompoundPluginDescriptorFinder()
                // Demo is using the Manifest file
                // PropertiesPluginDescriptorFinder is commented out just to avoid error log
                //.add( new PropertiesPluginDescriptorFinder() );
                .add( new ManifestPluginDescriptorFinder() );
    }


}
