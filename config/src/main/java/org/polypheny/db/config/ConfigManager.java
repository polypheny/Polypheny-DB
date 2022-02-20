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

package org.polypheny.db.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.exception.ConfigRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;


/**
 * ConfigManager allows to add and retrieve configuration objects.
 * If the configuration element has a Web UI Group and Web UI Page defined, it can be requested from the Web UI and the value
 * of the configuration can be changed there.
 */
@Slf4j
public class ConfigManager {

    private static final ConfigManager INSTANCE = new ConfigManager();

    private static final String DEFAULT_CONFIGURATION_FILE_NAME = "polypheny.conf";
    private static final String DEFAULT_CONFIGURATION_DIRECTORY_NAME = "config";

    public static boolean memoryMode = true; // If true, then changes are saved in-memory only and will be lost after restart.

    private static boolean usesExternalConfigFile = false;

    private static String currentConfigurationFileName;
    private static String currentConfigurationDirectoryName;

    private final ConcurrentMap<String, Config> configs;
    private final ConcurrentMap<String, WebUiGroup> uiGroups;
    private final ConcurrentMap<String, WebUiPage> uiPages;

    // Typesafe version of configuration file to be processed in code
    private static com.typesafe.config.Config configFile;

    // Actual File on disk
    private static File applicationConfFile = null;
    private static File applicationConfDir = null;


    private ConfigManager() {
        this.configs = new ConcurrentHashMap<>();
        this.uiGroups = new ConcurrentHashMap<>();
        this.uiPages = new ConcurrentHashMap<>();
    }


    /**
     * Singleton
     */
    public static ConfigManager getInstance() {
        if ( configFile == null ) {
            currentConfigurationFileName = DEFAULT_CONFIGURATION_FILE_NAME;
            currentConfigurationDirectoryName = DEFAULT_CONFIGURATION_DIRECTORY_NAME;
            loadConfigFile();
        }
        return INSTANCE;
    }


    public static void loadConfigFile() {
        // No custom location has been specified
        // Assume Default
        if ( applicationConfFile == null ) {
            initializeFileLocation();
        }
        configFile = ConfigFactory.parseFile( applicationConfFile );
    }


    private static void initializeFileLocation() {
        // Create config directory and file if they do not already exist
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        if ( applicationConfDir == null ) {
            applicationConfDir = homeDirManager.registerNewFolder( currentConfigurationDirectoryName );
        } else {
            applicationConfDir = homeDirManager.registerNewFolder( applicationConfDir.getParentFile(), currentConfigurationDirectoryName );
        }
        applicationConfFile = homeDirManager.registerNewFile( applicationConfDir, currentConfigurationFileName );
    }


    // Validates if configuration directory is still accessible
    private static boolean validateConfiguredFileLocation() {
        if ( applicationConfFile.exists() && applicationConfDir.exists() ) {
            // Although not beneficial for the system. It should not crash.
            // However, it should log an error to application log.
            PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
            if ( !homeDirManager.isAccessible( applicationConfFile ) ) {
                log.error( "Configuration Directory: {} or file: {} is not accessible. Config couldn't be updated.",
                        applicationConfDir.getAbsolutePath(),
                        applicationConfFile.getName() );
                return false;
            }
        } else {
            initializeFileLocation();
        }
        return true;
    }

    // TODO @HENNLO add method that recreated the entire conf file after deletion with all values that are not default
    //  shall be triggered by arbitrary config change


    private static void writeConfiguration( final com.typesafe.config.Config configuration ) {
        ConfigRenderOptions configRenderOptions = ConfigRenderOptions.defaults();
        configRenderOptions = configRenderOptions.setComments( false );
        configRenderOptions = configRenderOptions.setFormatted( true );
        configRenderOptions = configRenderOptions.setJson( false );
        configRenderOptions = configRenderOptions.setOriginComments( false );

        // Check if file is still accessible and writable
        if ( validateConfiguredFileLocation() ) {
            try (
                    FileOutputStream fos = new FileOutputStream( applicationConfFile, false );
                    BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( fos ) )
            ) {
                bw.write( configuration.root().render( configRenderOptions ) );
            } catch ( IOException e ) {
                log.error( "Exception while writing configuration file", e );
            }
            loadConfigFile();
        }
    }


    /**
     * Updates Config to file
     */
    public void persistConfigValue( String configKey, Object updatedValue ) {
        // TODO Extend with deviations from default Value, the actual defaultValue, description and link to website

        // Updated config that will be written to disk
        com.typesafe.config.Config newConfig;

        // Because lists with a size of 0 can't be written to config -- Error in typeconfig: ConfigImpl:269
        if ( !(updatedValue instanceof Collection && ((Collection<?>) updatedValue).size() == 0) ) {
            // Check if the new value is default value.
            // If so, the value will be omitted since there is no need to write it to file
            if ( configs.get( configKey ).isDefault() ) {
                //if ( updatedValue.toString().equals( configs.get( configKey ).getDefaultValue().toString() ) ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Updated value: '{}' for key: '{}' is equal to default value. Omitting.", updatedValue, configKey );
                }
                newConfig = configFile.withoutPath( configKey );
            } else {
                newConfig = parseConfigObject( configKey, updatedValue );
            }
            writeConfiguration( newConfig );
        }
    }


    private com.typesafe.config.Config parseConfigObject( String configKey, Object updatedValue ) {
        com.typesafe.config.Config modifiedConfig;
        if ( updatedValue instanceof Collection ) {
            Map<String, Object> myList = new HashMap<>();
            for ( Object value : (Collection) updatedValue ) {
                if ( (value instanceof ConfigDocker) ) {
                    Map<String, String> settingsMap = ((ConfigDocker) value).getSettings();
                    myList.put( ((ConfigObject) value).getKey(), settingsMap );
                }
            }
            modifiedConfig = configFile.withValue( configKey, ConfigValueFactory.fromAnyRef( myList ) );
        } else {
            modifiedConfig = configFile.withValue( configKey, ConfigValueFactory.fromAnyRef( updatedValue ) );
        }
        return modifiedConfig;
    }


    /**
     * Resets the config file back to the systems default.
     * This is mainly needed for testing purposes. To have no cross-site effects.
     */
    public void useDefaultApplicationConfFile() {
        // Resets applicationConfFile to null, in order to automatically reinitializes the config.
        applicationConfFile = null;
        applicationConfDir = null;

        currentConfigurationFileName = DEFAULT_CONFIGURATION_FILE_NAME;
        currentConfigurationDirectoryName = DEFAULT_CONFIGURATION_DIRECTORY_NAME;

        usesExternalConfigFile = false;
        loadConfigFile();
    }


    /**
     * Resets all persisted configurations in file back to default.
     */
    public void resetDefaultConfiguration() {
        useDefaultApplicationConfFile();
        if ( validateConfiguredFileLocation() ) {
            try (
                    FileOutputStream fos = new FileOutputStream( applicationConfFile, false );
                    BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( fos ) )
            ) {
                // Empty file contents
                bw.write( "" );
            } catch ( IOException e ) {
                log.error( "Exception while writing configuration file", e );
            }
        }
        loadConfigFile();
    }


    /**
     * Used to specify custom configuration files.
     *
     * @param customConfFile Configuration file
     */
    public static void setApplicationConfFile( File customConfFile ) {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        // If specified custom File is equal to the system default. Omit further processing and return to default
        if ( !customConfFile.equals( homeDirManager.getFileIfExists( DEFAULT_CONFIGURATION_DIRECTORY_NAME + "/" + DEFAULT_CONFIGURATION_FILE_NAME ) ) ) {
            if ( customConfFile.exists() && homeDirManager.isAccessible( customConfFile ) ) {
                applicationConfFile = customConfFile.getAbsoluteFile();

                currentConfigurationFileName = customConfFile.getName();
                currentConfigurationDirectoryName = customConfFile.getParentFile().getName();
                applicationConfDir = homeDirManager.registerNewFolder(
                        applicationConfFile.getParentFile().getParentFile(),
                        currentConfigurationDirectoryName );

                loadConfigFile();
                usesExternalConfigFile = true;
            } else {
                log.error( "The specified configuration file {} cannot be accessed or does not exist.", customConfFile.getAbsolutePath() );
                throw new ConfigRuntimeException( "The specified configuration file " + customConfFile.getAbsolutePath() + " cannot be accessed or does not exist." );
            }
        } else {
            log.warn( "The specified configuration file {} is the default. No need to specify specifically", customConfFile.getAbsolutePath() );
        }
    }


    /**
     * Register a configuration element in the ConfigManager.
     * Either the default value is used. Or if the key is present within the configuration file, this value will be used instead.
     *
     * @param config Configuration element to register.
     * @throws ConfigRuntimeException If a Config is already registered.
     */
    public void registerConfig( final Config config ) {
        if ( this.configs.containsKey( config.getKey() ) ) {
            throw new ConfigRuntimeException( "Cannot register two configuration elements with the same key: " + config.getKey() );
        } else {
            if ( !(memoryMode && !usesExternalConfigFile) ) {
                // Check if the config file contains this key and if so set the value to the one defined in the config file
                if ( configFile.hasPath( config.getKey() ) ) {
                    config.setValueFromFile( configFile );
                }
            }
            this.configs.put( config.getKey(), config );

            // Observe every registered config that if config is changed Manager gets notified and can persist the changed config
            config.addObserver( new ConfigManagerListener() );
        }
    }


    /**
     * Register multiple configuration elements in the ConfigManager.
     *
     * @param configs Configuration elements to register
     */
    public void registerConfigs( final Config... configs ) {
        for ( Config c : configs ) {
            this.registerConfig( c );
        }
    }


    public void observeAll( final ConfigListener listener ) {
        for ( Config c : configs.values() ) {
            c.addObserver( listener );
        }
    }


    /**
     * Get configuration as Configuration object.
     */
    public Config getConfig( final String s ) {
        return configs.get( s );
    }


    /**
     * Register a Web UI Group in the ConfigManager.
     * A Web UI Group consists of several Configs that will be displayed together in the Web UI.
     *
     * @param group WebUiGroup to register
     * @throws ConfigRuntimeException If a group with that key already exists.
     */
    public void registerWebUiGroup( final WebUiGroup group ) {
        if ( this.uiGroups.containsKey( group.getId() ) ) {
            throw new ConfigRuntimeException( "Cannot register two WeUiGroups with the same key: " + group.getId() );
        } else {
            this.uiGroups.put( group.getId(), group );
        }
    }


    /**
     * Register a Web UI Page in the ConfigManager.
     * A Web UI Page consists of several Web UI Groups that will be displayed together in the Web UI.
     *
     * @param page WebUiPage to register
     * @throws ConfigRuntimeException If a page with that key already exists.
     */
    public void registerWebUiPage( final WebUiPage page ) {
        if ( this.uiPages.containsKey( page.getId() ) ) {
            throw new ConfigRuntimeException( "Cannot register two WebUiPages with the same key: " + page.getId() );
        } else {
            this.uiPages.put( page.getId(), page );
        }
    }


    /**
     * Generates a Json of all the Web UI Pages in the ConfigManager (for the sidebar in the Web UI).
     * The Json does not contain the groups and configs of the Web UI Pages.
     */
    public String getWebUiPageList() {
        //todo recursion with parentPage field
        // Angular wants: { id, name, icon, children[] }
        ArrayList<PageListItem> out = new ArrayList<>();
        for ( WebUiPage p : uiPages.values() ) {
            out.add( new PageListItem( p.getId(), p.getTitle(), p.getIcon(), p.getLabel() ) );
        }
        out.sort( Comparator.comparing( PageListItem::getName ) );
        Gson gson = new GsonBuilder()
                .enableComplexMapKeySerialization()
                .serializeNulls()
                .create();
        return gson.toJson( out );
    }


    /**
     * Get certain page as json.
     * Groups within a page and configs within a group are sorted in the Web UI, not here.
     *
     * @param id The id of the page
     */
    public String getPage( final String id ) {
        // Fill WebUiGroups with Configs
        for ( ConcurrentMap.Entry<String, Config> c : configs.entrySet() ) {
            try {
                String i = c.getValue().getWebUiGroup();
                this.uiGroups.get( i ).addConfig( c.getValue() );
            } catch ( NullPointerException e ) {
                // TODO: This is not nice...
                // Skipping config with no WebUiGroup
            }
        }

        // Fill WebUiPages with WebUiGroups
        for ( ConcurrentMap.Entry<String, WebUiGroup> g : uiGroups.entrySet() ) {
            try {
                String i = g.getValue().getPageId();
                this.uiPages.get( i ).addWebUiGroup( g.getValue() );
            } catch ( NullPointerException e ) {
                // TODO: This is not nice...
                // Skipping config with no page id
            }
        }
        return uiPages.get( id ).toString();
    }


    public String getActiveConfFile() {
        return applicationConfFile.getAbsolutePath();
    }


    static class ConfigManagerListener implements ConfigListener {

        @Override
        public void onConfigChange( Config c ) {
            if ( !memoryMode ) {
                INSTANCE.persistConfigValue( c.getKey(), c.getPlainValueObject() );
            }
        }


        @Override
        public void restart( Config c ) {

        }

    }


    /**
     * The class PageListItem will be converted into a Json String by Gson.
     * The Web UI requires a Json Object with the fields id, name, icon, children[] for the Sidebar.
     * This class is required to convert a WebUiPage object into the format needed by the Angular WebUi.
     */
    static class PageListItem {

        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String id;
        @Getter
        private String name;
        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String icon;
        @SuppressWarnings({ "FieldCanBeLocal", "unused" })
        private String label;
        @SuppressWarnings({ "unused" })
        private PageListItem[] children;


        PageListItem( final String id, final String name, final String icon, final String label ) {
            this.id = id;
            this.name = name;
            this.icon = icon;
            this.label = label;
        }


        @Override
        public String toString() {
            Gson gson = new Gson();
            return gson.toJson( this );
        }

    }

}
