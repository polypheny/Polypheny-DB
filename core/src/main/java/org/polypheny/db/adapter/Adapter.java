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

package org.polypheny.db.adapter;


import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigObject;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;

@Getter
public abstract class Adapter {

    private final AdapterProperties properties;
    protected final DeployMode deployMode;
    protected String deploymentId;
    private final List<NamespaceType> supportedNamespaceTypes;
    @Getter
    private final String adapterName;


    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterProperties {

        String name();

        String description();

        DeployMode[] usedModes();

        NamespaceType[] supportedNamespaceTypes() default { NamespaceType.RELATIONAL };

    }


    @Inherited
    @Target(ElementType.TYPE)
    @Repeatable(AdapterSettingString.List.class)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterSettingString {

        String name();

        boolean canBeNull() default false;

        boolean required() default true;

        boolean modifiable() default false;

        String description() default "";

        String defaultValue();

        int position() default 100;

        DeploySetting[] appliesTo() default DeploySetting.DEFAULT;

        String subOf() default "";

        @Inherited
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @interface List {

            AdapterSettingString[] value();

        }

    }


    @Inherited
    @Repeatable(AdapterSettingInteger.List.class)
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterSettingInteger {

        String name();

        boolean canBeNull() default false;

        boolean required() default true;

        boolean modifiable() default false;

        String description() default "";

        int defaultValue();

        int position() default 100;

        DeploySetting[] appliesTo() default DeploySetting.DEFAULT;

        String subOf() default "";

        @Inherited
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @interface List {

            AdapterSettingInteger[] value();

        }

    }


    @Inherited
    @Repeatable(AdapterSettingBoolean.List.class)
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterSettingBoolean {

        String name();

        boolean canBeNull() default false;

        boolean required() default true;

        boolean modifiable() default false;

        String description() default "";

        boolean defaultValue();

        int position() default 100;

        DeploySetting[] appliesTo() default DeploySetting.DEFAULT;

        String subOf() default "";

        @Inherited
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @interface List {

            AdapterSettingBoolean[] value();

        }

    }


    @Inherited
    @Repeatable(AdapterSettingList.List.class)
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterSettingList {

        String name();

        boolean canBeNull() default false;

        boolean required() default true;

        boolean modifiable() default false;

        String description() default "";

        String defaultValue();

        String[] options();


        int position() default 100;

        DeploySetting[] appliesTo() default DeploySetting.DEFAULT;

        String subOf() default "";

        @Inherited
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @interface List {

            AdapterSettingList[] value();

        }

    }


    @Inherited
    @Target(ElementType.TYPE)
    @Repeatable(AdapterSettingDirectory.List.class)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface AdapterSettingDirectory {

        String name();

        boolean canBeNull() default false;

        boolean required() default true;

        boolean modifiable() default false;

        String description() default "";

        int position() default 100;

        DeploySetting[] appliesTo() default DeploySetting.DEFAULT;

        String subOf() default "";

        @Inherited
        @Target(ElementType.TYPE)
        @Retention(RetentionPolicy.RUNTIME)
        @interface List {

            AdapterSettingDirectory[] value();

        }

    }


    @Getter
    private final int adapterId;
    @Getter
    private final String uniqueName;

    protected final Map<String, String> settings;

    protected final InformationPage informationPage;
    protected final List<InformationGroup> informationGroups;
    protected final List<Information> informationElements;
    private ConfigListener listener;


    public Adapter( int adapterId, String uniqueName, Map<String, String> settings ) {
        this.properties = getClass().getAnnotation( AdapterProperties.class );
        if ( getClass().getAnnotation( AdapterProperties.class ) == null ) {
            throw new RuntimeException( "The used adapter does not annotate its properties correctly." );
        }
        if ( !settings.containsKey( "mode" ) ) {
            throw new RuntimeException( "The adapter does not specify a mode which is necessary." );
        }

        this.supportedNamespaceTypes = Arrays.asList( properties.supportedNamespaceTypes() );

        this.deployMode = DeployMode.fromString( settings.get( "mode" ) );

        this.adapterId = adapterId;
        this.uniqueName = uniqueName;
        this.adapterName = properties.name();
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = new HashMap<>( settings );

        informationPage = new InformationPage( uniqueName );
        informationGroups = new ArrayList<>();
        informationElements = new ArrayList<>();

        // this is need for docker deployable stores and should not interfere too much with other adapters
        if ( deployMode == DeployMode.DOCKER ) {
            this.listener = attachListener( Integer.parseInt( settings.get( "instanceId" ) ) );
        }
    }


    public abstract void createNewSchema( SchemaPlus rootSchema, String name );

    public abstract Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement );

    public abstract Schema getCurrentSchema();


    public void createGraphNamespace( SchemaPlus rootSchema, String name, long id ) {
        throw new UnsupportedOperationException( "It is not supported to create a graph with this adapter." );
    }


    public Table createDocumentSchema( CatalogCollection catalogEntity, CatalogCollectionPlacement partitionPlacement ) {
        throw new UnsupportedOperationException( "It is not supported to create a document with this adapter." );
    }


    public Schema getCurrentGraphNamespace() {
        throw new UnsupportedOperationException( "It is not supported to create a graph with this adapter." );
    }


    public abstract void truncate( Context context, CatalogTable table );

    public abstract boolean prepare( PolyXid xid );

    public abstract void commit( PolyXid xid );

    public abstract void rollback( PolyXid xid );


    public List<AbstractAdapterSetting> getAvailableSettings( Class<?> clazz ) {
        return AbstractAdapterSetting.fromAnnotations( clazz.getAnnotations(), properties )
                .values()
                .stream()
                .flatMap( Collection::stream )
                .collect( Collectors.toList() );
    }


    public static Map<String, String> getDefaultSettings( Class<DataStore> clazz ) {
        return AbstractAdapterSetting.fromAnnotations( clazz.getAnnotations(), clazz.getAnnotation( AdapterProperties.class ) )
                .values()
                .stream()
                .flatMap( Collection::stream )
                .collect( Collectors.toMap( e -> e.name, e -> e.defaultValue ) );
    }


    public void shutdownAndRemoveListeners() {
        shutdown();
        if ( deployMode == DeployMode.DOCKER ) {
            DockerManager.getInstance().removeListener( this.listener );
            DockerContainer.getContainerByUUID( deploymentId ).ifPresent( DockerContainer::destroy );
        }
    }


    public abstract void shutdown();


    /**
     * Informs a store that its settings have changed.
     *
     * @param updatedSettings List of setting names that have changed.
     */
    protected abstract void reloadSettings( List<String> updatedSettings );


    protected List<String> applySettings( Map<String, String> newSettings ) {
        List<String> updatedSettings = new ArrayList<>();
        for ( Entry<String, String> newSetting : newSettings.entrySet() ) {
            if ( !Objects.equals( this.settings.get( newSetting.getKey() ), newSetting.getValue() ) ) {
                this.settings.put( newSetting.getKey(), newSetting.getValue() );
                updatedSettings.add( newSetting.getKey() );
            }
        }

        return updatedSettings;
    }


    public void updateSettings( Map<String, String> newSettings ) {
        this.validateSettings( newSettings, false );
        List<String> updatedSettings = this.applySettings( newSettings );
        this.reloadSettings( updatedSettings );
        Catalog catalog = Catalog.getInstance();
        catalog.updateAdapterSettings( getAdapterId(), getCurrentSettings() );
        try {
            catalog.commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( e );
        }
    }


    public Map<String, String> getCurrentSettings() {
        // we unwrap the dockerInstance details here, for convenience
        return settings;
    }


    protected void validateSettings( Map<String, String> newSettings, boolean initialSetup ) {
        for ( AbstractAdapterSetting s : getAvailableSettings( getClass() ) ) {
            // we only need to check settings which apply to the used mode
            if ( !s.appliesTo
                    .stream()
                    .map( setting -> setting.getModes( Arrays.asList( properties.usedModes() ) ) )
                    .collect( Collectors.toList() ).contains( deployMode ) ) {
                continue;
            }
            if ( newSettings.containsKey( s.name ) ) {
                if ( s.modifiable || initialSetup ) {
                    String newValue = newSettings.get( s.name );
                    if ( !s.canBeNull && newValue == null ) {
                        throw new RuntimeException( "Setting \"" + s.name + "\" cannot be null." );
                    }
                } else {
                    throw new RuntimeException( "Setting \"" + s.name + "\" cannot be modified." );
                }
            } else if ( s.required && initialSetup ) {
                throw new RuntimeException( "Setting \"" + s.name + "\" must be present." );
            }
        }
    }


    /**
     * Enables the previously configured information groups and elements for this adapter in the InformationManager
     */
    public void enableInformationPage() {
        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        informationGroups.forEach( im::addGroup );

        for ( Information information : informationElements ) {
            im.registerInformation( information );
        }
    }


    /**
     * Removes all information objects defined in this adapter from the InformationManager
     */
    public void removeInformationPage() {
        if ( informationElements.size() > 0 ) {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( informationElements.toArray( new Information[0] ) );
            informationGroups.forEach( im::removeGroup );
            im.removePage( informationPage );
        }
    }


    /**
     * Builds and adds a new information group, observing physical naming of columns, to the provided information objects
     */
    public void addInformationPhysicalNames() {
        InformationGroup group = new InformationGroup( informationPage, "Physical Names" );
        InformationTable physicalColumnNames = new InformationTable(
                group,
                Arrays.asList( "Id", "Name", "Physical Name" )
        );
        informationElements.add( physicalColumnNames );

        Catalog catalog = Catalog.getInstance();
        group.setRefreshFunction( () -> {
            physicalColumnNames.reset();
            List<CatalogPartitionPlacement> cpps = catalog.getPartitionPlacementsByAdapter( adapterId );
            cpps.forEach( cpp ->
                    catalog.getColumnPlacementsOnAdapterPerTable( adapterId, cpp.tableId ).forEach( placement -> {
                        physicalColumnNames.addRow(
                                placement.columnId,
                                catalog.getColumn( placement.columnId ).name,
                                cpp.physicalSchemaName + "." + cpp.physicalTableName + "." + placement.physicalColumnName );
                    } )
            );
        } );

        informationGroups.add( group );
    }


    /**
     * This function attaches the callee to the specified docker instance,
     * it will call the appropriate resetConnection function when the Docker configuration changes
     *
     * @param dockerInstanceId the id of the corresponding Docker instance
     */
    ConfigListener attachListener( int dockerInstanceId ) {
        // we have to track the used docker url we attach a listener
        ConfigListener listener = new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                resetDockerConnection();
            }


            @Override
            public void restart( Config c ) {
                resetDockerConnection();
            }
        };
        DockerManager.getInstance().addListener( listener );
        return listener;
    }


    /**
     * This function is called automatically if the configuration of connected Docker instance changes,
     * it is responsible for handling regenerating the connection if the Docker changes demand it
     */
    protected void resetDockerConnection() {
        throw new RuntimeException( getUniqueName() + " uses this Docker instance and does not support to dynamically change it." );
    }


    @Accessors(chain = true)
    public static abstract class AbstractAdapterSetting {

        public final String name;
        public final boolean canBeNull;
        public final String subOf;
        public final boolean required;
        public final boolean modifiable;
        public final String defaultValue;
        private final int position;
        @Setter
        public String description;

        @Getter
        private final List<DeploySetting> appliesTo;


        public AbstractAdapterSetting( final String name, final boolean canBeNull, final String subOf, final boolean required, final boolean modifiable, List<DeploySetting> appliesTo, String defaultValue, int position ) {
            this.name = name;
            this.canBeNull = canBeNull;
            this.subOf = Objects.equals( subOf, "" ) ? null : subOf;
            this.required = required;
            this.modifiable = modifiable;
            this.position = position;
            this.appliesTo = appliesTo;
            this.defaultValue = defaultValue;
            assert this.subOf == null || this.subOf.split( "_" ).length == 2
                    : "SubOf needs to be null or has to be seperated by \"_\" and requires link and value due to limitation in Java";
        }


        /**
         * Method generates the correlated AdapterSettings from the provided AdapterAnnotations,
         * Repeatable Annotations are packed inside the underlying Lists of each AdapterSetting
         * as those AdapterSettings belong to a specific adapter the AdapterProperties are used to
         * unpack DeploySettings.ALL to the available modes correctly
         *
         * @param annotations collection of annotations
         * @param properties which are defined by the corresponding Adapter
         * @return a map containing the available modes and the corresponding collections of AdapterSettings
         */
        public static Map<String, List<AbstractAdapterSetting>> fromAnnotations( Annotation[] annotations, AdapterProperties properties ) {
            Map<String, List<AbstractAdapterSetting>> settings = new HashMap<>();

            for ( Annotation annotation : annotations ) {
                if ( annotation instanceof AdapterSettingString ) {
                    mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingString.fromAnnotation( (AdapterSettingString) annotation ) );
                } else if ( annotation instanceof AdapterSettingString.List ) {
                    Arrays.stream( ((AdapterSettingString.List) annotation).value() ).forEach( el -> mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingString.fromAnnotation( el ) ) );
                } else if ( annotation instanceof AdapterSettingBoolean ) {
                    mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingBoolean.fromAnnotation( (AdapterSettingBoolean) annotation ) );
                } else if ( annotation instanceof AdapterSettingBoolean.List ) {
                    Arrays.stream( ((AdapterSettingBoolean.List) annotation).value() ).forEach( el -> mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingBoolean.fromAnnotation( el ) ) );
                } else if ( annotation instanceof AdapterSettingInteger ) {
                    mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingInteger.fromAnnotation( (AdapterSettingInteger) annotation ) );
                } else if ( annotation instanceof AdapterSettingInteger.List ) {
                    Arrays.stream( ((AdapterSettingInteger.List) annotation).value() ).forEach( el -> mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingInteger.fromAnnotation( el ) ) );
                } else if ( annotation instanceof AdapterSettingList ) {
                    mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingList.fromAnnotation( (AdapterSettingList) annotation ) );
                } else if ( annotation instanceof AdapterSettingList.List ) {
                    Arrays.stream( ((AdapterSettingList.List) annotation).value() ).forEach( el -> mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingList.fromAnnotation( el ) ) );
                } else if ( annotation instanceof AdapterSettingDirectory ) {
                    mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingDirectory.fromAnnotation( (AdapterSettingDirectory) annotation ) );
                } else if ( annotation instanceof AdapterSettingDirectory.List ) {
                    Arrays.stream( ((AdapterSettingDirectory.List) annotation).value() ).forEach( el -> mergeSettings( settings, properties.usedModes(), AbstractAdapterSettingDirectory.fromAnnotation( el ) ) );
                }
            }

            settings.forEach( ( key, values ) -> values.sort( Comparator.comparingInt( value -> value.position ) ) );
            return settings;
        }


        /**
         * Merges the provided setting into the provided map of AdapterSettings
         *
         * @param settings already correctly sorted settings
         * @param deployModes the deployment modes which are supported by this specific adapter
         * @param setting the setting which is merged into the map
         */
        private static void mergeSettings( Map<String, List<AbstractAdapterSetting>> settings, DeployMode[] deployModes, AbstractAdapterSetting setting ) {
            // we need to unpack the underlying DeployModes
            for ( DeployMode mode : setting.appliesTo
                    .stream()
                    .flatMap( mode -> mode.getModes( Arrays.asList( deployModes ) ).stream() )
                    .collect( Collectors.toList() ) ) {

                if ( settings.containsKey( mode.getName() ) ) {
                    settings.get( mode.getName() ).add( setting );
                } else {
                    List<AbstractAdapterSetting> temp = new ArrayList<>();
                    temp.add( setting );
                    settings.put( mode.getName(), temp );
                }
            }
        }


        /**
         * In most subclasses, this method returns the defaultValue, because the UI overrides the defaultValue when a new value is set.
         */
        public abstract String getValue();


        public static List<AbstractAdapterSetting> serializeSettings( List<AbstractAdapterSetting> availableSettings, Map<String, String> currentSettings ) {
            ArrayList<AbstractAdapterSetting> abstractAdapterSettings = new ArrayList<>();
            for ( AbstractAdapterSetting s : availableSettings ) {
                for ( String current : currentSettings.keySet() ) {
                    if ( s.name.equals( current ) ) {
                        abstractAdapterSettings.add( s );
                    }
                }
            }
            return abstractAdapterSettings;
        }


    }


    public static class AbstractAdapterSettingInteger extends AbstractAdapterSetting {

        private final String type = "Integer";


        public AbstractAdapterSettingInteger( String name, boolean canBeNull, final String subOf, boolean required, boolean modifiable, Integer defaultValue, List<DeploySetting> modes, int position ) {
            super( name, canBeNull, subOf, required, modifiable, modes, defaultValue.toString(), position );
        }


        public static AbstractAdapterSetting fromAnnotation( AdapterSettingInteger annotation ) {
            return new AbstractAdapterSettingInteger(
                    annotation.name(),
                    annotation.canBeNull(),
                    annotation.subOf(),
                    annotation.required(),
                    annotation.modifiable(),
                    annotation.defaultValue(),
                    Arrays.asList( annotation.appliesTo() ),
                    annotation.position() );
        }


        @Override
        public String getValue() {
            return defaultValue;
        }

    }


    public static class AbstractAdapterSettingString extends AbstractAdapterSetting {

        private final String type = "String";


        public AbstractAdapterSettingString( String name, boolean canBeNull, String sub, boolean required, boolean modifiable, String defaultValue, List<DeploySetting> modes, int position ) {
            super( name, canBeNull, sub, required, modifiable, modes, defaultValue, position );
        }


        public static AbstractAdapterSetting fromAnnotation( AdapterSettingString annotation ) {
            return new AbstractAdapterSettingString(
                    annotation.name(),
                    annotation.canBeNull(),
                    annotation.subOf(),
                    annotation.required(),
                    annotation.modifiable(),
                    annotation.defaultValue(),
                    Arrays.asList( annotation.appliesTo() ),
                    annotation.position() );
        }


        @Override
        public String getValue() {
            return defaultValue;
        }

    }


    public static class AbstractAdapterSettingBoolean extends AbstractAdapterSetting {

        private final String type = "Boolean";


        public AbstractAdapterSettingBoolean( String name, boolean canBeNull, final String sub, boolean required, boolean modifiable, boolean defaultValue, List<DeploySetting> modes, int position ) {
            super( name, canBeNull, sub, required, modifiable, modes, String.valueOf( defaultValue ), position );
        }


        public static AbstractAdapterSettingBoolean fromAnnotation( AdapterSettingBoolean annotation ) {
            return new AbstractAdapterSettingBoolean(
                    annotation.name(),
                    annotation.canBeNull(),
                    annotation.subOf(),
                    annotation.required(),
                    annotation.modifiable(),
                    annotation.defaultValue(),
                    Arrays.asList( annotation.appliesTo() ),
                    annotation.position() );
        }


        @Override
        public String getValue() {
            return defaultValue;
        }

    }


    @Accessors(chain = true)
    public static class AbstractAdapterSettingList extends AbstractAdapterSetting {

        private final String type = "List";
        public List<String> options;
        public boolean dynamic = false;


        public AbstractAdapterSettingList( String name, boolean canBeNull, final String subOf, boolean required, boolean modifiable, List<String> options, List<DeploySetting> modes, String defaultValue, int position ) {
            super( name, canBeNull, subOf, required, modifiable, modes, defaultValue, position );
            this.options = options;
        }


        public static AbstractAdapterSetting fromAnnotation( AdapterSettingList annotation ) {
            return new AbstractAdapterSettingList(
                    annotation.name(),
                    annotation.canBeNull(),
                    annotation.subOf(),
                    annotation.required(),
                    annotation.modifiable(),
                    Arrays.asList( annotation.options() ),
                    Arrays.asList( annotation.appliesTo() ),
                    annotation.defaultValue(),
                    annotation.position() );
        }


        @Override
        public String getValue() {
            return defaultValue;
        }

    }


    /**
     * BindableSettingsList which allows to configure mapped AdapterSettings, which expose an alias in the frontend
     * but assign a corresponding id when the value is chosen
     *
     * @param <T>
     */
    @Accessors(chain = true)
    public static class BindableAbstractAdapterSettingsList<T extends ConfigObject> extends AbstractAdapterSettingList {

        private final transient Function<T, String> mapper;
        private final transient Class<T> clazz;
        private Map<Integer, String> alias;
        private final String nameAlias;
        public RuntimeConfig boundConfig;


        public BindableAbstractAdapterSettingsList( String name, String nameAlias, boolean canBeNull, String subOf, boolean required, boolean modifiable, List<T> options, Function<T, String> mapper, Class<T> clazz ) {
            super( name, canBeNull, subOf, required, modifiable, options.stream().map( ( el ) -> String.valueOf( el.getId() ) ).collect( Collectors.toList() ), new ArrayList<>(), null, 1000 );
            this.mapper = mapper;
            this.clazz = clazz;
            this.dynamic = true;
            this.nameAlias = nameAlias;
            this.alias = options.stream().collect( Collectors.toMap( ConfigObject::getId, mapper ) );
        }


        /**
         * This allows to bind this option to an existing RuntimeConfig,
         * which will update when the bound option changes
         *
         * @param config the RuntimeConfig which is bound
         * @return chain method to use the object
         */
        public AbstractAdapterSetting bind( RuntimeConfig config ) {
            this.boundConfig = config;
            ConfigListener listener = new ConfigListener() {
                @Override
                public void onConfigChange( Config c ) {
                    refreshFromConfig();
                }


                @Override
                public void restart( Config c ) {
                    refreshFromConfig();
                }
            };
            config.addObserver( listener );

            return this;
        }


        public void refreshFromConfig() {
            if ( boundConfig != null ) {
                options = boundConfig.getList( clazz ).stream().map( ( el ) -> String.valueOf( el.id ) ).collect( Collectors.toList() );
                alias = boundConfig.getList( clazz ).stream().collect( Collectors.toMap( ConfigObject::getId, mapper ) );
            }
        }


    }


    @Accessors(chain = true)
    public static class AbstractAdapterSettingDirectory extends AbstractAdapterSetting {

        private final String type = "Directory";
        @Setter
        public String directory;
        //This field is necessary for the UI and needs to be initialized to be serialized to JSON.
        @Setter
        public String[] fileNames = new String[]{ "" };
        public transient final Map<String, InputStream> inputStreams;


        public AbstractAdapterSettingDirectory( String name, boolean canBeNull, final String subOf, boolean required, boolean modifiable, List<DeploySetting> modes, int position ) {
            super( name, canBeNull, subOf, required, modifiable, modes, null, position );
            //so it will be serialized
            this.directory = "";
            this.inputStreams = new HashMap<>();
        }


        public static AbstractAdapterSetting fromAnnotation( AdapterSettingDirectory annotation ) {
            return new AbstractAdapterSettingDirectory(
                    annotation.name(),
                    annotation.canBeNull(),
                    annotation.subOf(),
                    annotation.required(),
                    annotation.modifiable(),
                    Arrays.asList( annotation.appliesTo() ),
                    annotation.position()
            );
        }


        @Override
        public String getValue() {
            return directory;
        }

    }


    //see https://stackoverflow.com/questions/19588020/gson-serialize-a-list-of-polymorphic-objects/22081826#22081826
    public static class AdapterSettingDeserializer implements JsonDeserializer<AbstractAdapterSetting> {

        @Override
        public AbstractAdapterSetting deserialize( JsonElement json, Type typeOfT, JsonDeserializationContext context ) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();
            String type = jsonObject.get( "type" ).getAsString();
            String name = jsonObject.get( "name" ).getAsString();
            boolean canBeNull = jsonObject.get( "canBeNull" ).getAsBoolean();
            boolean required = jsonObject.get( "required" ).getAsBoolean();
            boolean modifiable = jsonObject.get( "modifiable" ).getAsBoolean();
            String subOf = jsonObject.has( "subOf" ) ? jsonObject.get( "subOf" ).getAsString() : null;
            int position = jsonObject.get( "position" ).getAsInt();
            String defaultValue;
            String description = null;
            if ( jsonObject.get( "description" ) != null ) {
                description = jsonObject.get( "description" ).getAsString();
            }

            AbstractAdapterSetting out;
            switch ( type ) {
                case "Integer":
                    Integer integer = jsonObject.get( "defaultValue" ).getAsInt();
                    out = new AbstractAdapterSettingInteger( name, canBeNull, subOf, required, modifiable, integer, new ArrayList<>(), position );
                    break;
                case "String":
                    String string = jsonObject.get( "defaultValue" ).getAsString();
                    out = new AbstractAdapterSettingString( name, canBeNull, subOf, required, modifiable, string, new ArrayList<>(), position );
                    break;
                case "Boolean":
                    boolean bool = jsonObject.get( "defaultValue" ).getAsBoolean();
                    out = new AbstractAdapterSettingBoolean( name, canBeNull, subOf, required, modifiable, bool, new ArrayList<>(), position );
                    break;
                case "List":
                    List<String> options = context.deserialize( jsonObject.get( "options" ), List.class );
                    defaultValue = context.deserialize( jsonObject.get( "defaultValue" ), String.class );
                    out = new AbstractAdapterSettingList( name, canBeNull, subOf, required, modifiable, options, new ArrayList<>(), defaultValue, position );
                    break;
                case "Directory":
                    String directory = context.deserialize( jsonObject.get( "directory" ), String.class );
                    String[] fileNames = context.deserialize( jsonObject.get( "fileNames" ), String[].class );
                    out = new AbstractAdapterSettingDirectory( name, canBeNull, subOf, required, modifiable, new ArrayList<>(), position ).setDirectory( directory ).setFileNames( fileNames );
                    break;
                default:
                    throw new RuntimeException( "Could not deserialize AdapterSetting of type " + type );
            }
            out.setDescription( description );
            return out;
        }

    }

}
