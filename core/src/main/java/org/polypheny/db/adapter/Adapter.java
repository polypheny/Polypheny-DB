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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.transaction.PolyXid;

@Getter
@Slf4j
public abstract class Adapter<S extends StoreCatalog> implements Scannable, Modifiable, Expressible {

    private final AdapterProperties properties;
    protected final DeployMode deployMode;
    @Getter
    private final String adapterName;
    public final S storeCatalog;


    @Getter
    public final long adapterId;
    @Getter
    private final String uniqueName;

    protected final Map<String, String> settings;

    protected final InformationPage informationPage;
    protected final List<InformationGroup> informationGroups;
    protected final List<Information> informationElements;
    private ConfigListener listener;

    private final Map<Long, Namespace> namespaces = new ConcurrentHashMap<>();


    public Adapter( long adapterId, String uniqueName, Map<String, String> settings, S catalog ) {
        this.storeCatalog = catalog;
        this.properties = getClass().getAnnotation( AdapterProperties.class );
        if ( getClass().getAnnotation( AdapterProperties.class ) == null ) {
            throw new RuntimeException( "The used adapter does not annotate its properties correctly." );
        }
        if ( !settings.containsKey( "mode" ) ) {
            throw new RuntimeException( "The adapter does not specify a mode which is necessary." );
        }

        this.deployMode = DeployMode.fromString( settings.get( "mode" ) );

        this.adapterId = adapterId;
        this.uniqueName = uniqueName;
        this.adapterName = properties.name();
        // Make sure the settings are actually valid
        this.validateSettings( settings, true );
        this.settings = settings;

        informationPage = new InformationPage( uniqueName );
        informationGroups = new ArrayList<>();
        informationElements = new ArrayList<>();

        // this is need for docker deployable stores and should not interfere too much with other adapters
        if ( deployMode == DeployMode.DOCKER ) {
            this.listener = attachListener( Integer.parseInt( settings.get( "instanceId" ) ) );
        }
        Catalog.getInstance().addStoreSnapshot( catalog );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( AdapterManager.ADAPTER_MANAGER_EXPRESSION, "getAdapter", Expressions.constant( adapterId ) );
    }


    public Namespace getNamespace( long id ) {
        return namespaces.get( id );
    }


    public void putNamespace( Namespace namespace ) {
        this.namespaces.put( namespace.getId(), namespace );
    }


    public Expression getNamespaceAsExpression( long id ) {
        return Expressions.call( asExpression(), "getNamespace", Expressions.constant( id ) );
    }


    public abstract void updateNamespace( String name, long id );

    public abstract Namespace getCurrentNamespace();

    public abstract void truncate( Context context, long allocId );

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


    public static Map<String, String> getDefaultSettings( Class<DataStore<?>> clazz ) {
        return AbstractAdapterSetting.fromAnnotations( clazz.getAnnotations(), clazz.getAnnotation( AdapterProperties.class ) )
                .values()
                .stream()
                .flatMap( Collection::stream )
                .collect( Collectors.toMap( e -> e.name, e -> e.defaultValue ) );
    }


    public void shutdownAndRemoveListeners() {
        shutdown();
        if ( deployMode == DeployMode.DOCKER ) {
            RuntimeConfig.DOCKER_INSTANCES.removeObserver( this.listener );
        }
        DockerManager.getInstance().destroyAll( getAdapterId() );
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
        Catalog.getInstance().updateAdapterSettings( getAdapterId(), newSettings );
    }


    public Map<String, String> getCurrentSettings() {
        // we unwrap the dockerInstance details here, for convenience
        if ( deployMode == DeployMode.DOCKER ) {
            Map<String, String> dockerSettings = RuntimeConfig.DOCKER_INSTANCES
                    .getWithId( ConfigDocker.class, Integer.parseInt( settings.get( "instanceId" ) ) ).getSettings();
            dockerSettings.putAll( settings );
            return dockerSettings;
        }
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

        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        group.setRefreshFunction( () -> {
            physicalColumnNames.reset();
            List<PhysicalEntity> physicalsOnAdapter = new ArrayList<>();//snapshot.physical().getPhysicalsOnAdapter( adapterId );

            for ( PhysicalEntity entity : physicalsOnAdapter ) {
                if ( entity.namespaceType != NamespaceType.RELATIONAL ) {
                    continue;
                }
                PhysicalTable physicalTable = (PhysicalTable) entity;

                for ( PhysicalColumn column : physicalTable.columns ) {
                    physicalColumnNames.addRow(
                            column.id,
                            column.name,
                            physicalTable.namespaceName + "." + physicalTable.name + "." + column.name );
                }
            }
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
                List<ConfigDocker> configs = RuntimeConfig.DOCKER_INSTANCES.getList( ConfigDocker.class );
                if ( !configs.stream().map( conf -> conf.id ).collect( Collectors.toList() ).contains( dockerInstanceId ) ) {
                    throw new RuntimeException( "This DockerInstance has adapters on it, while this is the case it can not be deleted." );
                }
                resetDockerConnection( RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerInstanceId ) );
            }


            @Override
            public void restart( Config c ) {
                resetDockerConnection( RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, dockerInstanceId ) );
            }
        };
        RuntimeConfig.DOCKER_INSTANCES.addObserver( listener );
        return listener;
    }


    /**
     * This function is called automatically if the configuration of connected Docker instance changes,
     * it is responsible for handling regenerating the connection if the Docker changes demand it
     *
     * @param c the new configuration of the corresponding Docker instance
     */
    protected void resetDockerConnection( ConfigDocker c ) {
        throw new RuntimeException( getUniqueName() + " uses this Docker instance and does not support to dynamically change it." );
    }


}
