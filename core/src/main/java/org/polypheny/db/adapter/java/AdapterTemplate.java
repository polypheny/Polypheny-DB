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

package org.polypheny.db.adapter.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Value;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSettingList;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.DeployFn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingsPreset;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.DockerManager;

@Value
public class AdapterTemplate {

    public Class<?> clazz;
    public String adapterName;
    public AdapterType adapterType;
    DeployFn deployer;
    public List<AbstractAdapterSetting> settings;
    public List<DeployMode> modes;
    public List<AdapterSettingsPreset> presets;
    public long id;
    public String description;


    public AdapterTemplate( long id, Class<?> clazz, String adapterName, List<AbstractAdapterSetting> settings, List<DeployMode> modes, List<AdapterSettingsPreset> presets, String description, DeployFn deployer ) {
        this.id = id;
        this.adapterName = adapterName;
        this.description = description;
        this.clazz = clazz;
        this.settings = settings;
        this.modes = modes;
        this.presets = presets;
        this.adapterType = getAdapterType( clazz );
        this.deployer = deployer;
    }


    public static AdapterType getAdapterType( Class<?> clazz ) {
        return DataStore.class.isAssignableFrom( clazz ) ? AdapterType.STORE : AdapterType.SOURCE;
    }


    public static AdapterTemplate fromString( String adapterName, AdapterType adapterType ) {
        return AdapterManager.getAdapterTemplate( adapterName, adapterType );
    }


    public static List<AbstractAdapterSetting> getAllSettings( Class<? extends Adapter<?>> clazz ) {
        AdapterProperties properties = clazz.getAnnotation( AdapterProperties.class );
        if ( clazz.getAnnotation( AdapterProperties.class ) == null ) {
            throw new GenericRuntimeException( "The used adapter does not annotate its properties correctly." );
        }
        List<AbstractAdapterSetting> settings = new ArrayList<>( AbstractAdapterSetting.fromAnnotations( clazz.getAnnotations() ) );
        if ( Arrays.stream( properties.usedModes() ).anyMatch( m -> m == DeployMode.DOCKER ) ) {
            String instanceId = DockerManager.getInstance().getDockerInstances().keySet().stream().findFirst().orElse( 0 ).toString();
            List<String> ids = DockerManager.getInstance().getDockerInstances().keySet().stream().map( Object::toString ).toList();
            settings.add( new AbstractAdapterSettingList( "instanceId", false, null, true, false, ids, List.of( DeploySetting.DOCKER ), instanceId, 0, null ) );
        }
        return settings;
    }


    /**
     * Collects the deployment presets declared via {@link AdapterSettingsPreset} on the adapter class
     * and validates them against the adapter's settings and supported deploy modes.
     */
    public static List<AdapterSettingsPreset> getAllPresets( Class<? extends Adapter<?>> clazz, List<AbstractAdapterSetting> settings, List<DeployMode> modes ) {
        List<AdapterSettingsPreset> presets = Arrays.asList( clazz.getAnnotationsByType( AdapterSettingsPreset.class ) );
        for ( AdapterSettingsPreset preset : presets ) {
            if ( !modes.contains( preset.mode() ) ) {
                throw new GenericRuntimeException( "Preset '%s' of adapter %s uses deploy mode %s, which the adapter does not support.", preset.name(), clazz.getSimpleName(), preset.mode() );
            }
            for ( AdapterSettingsPreset.Setting entry : preset.settings() ) {
                AbstractAdapterSetting setting = settings.stream()
                        .filter( s -> s.name.equals( entry.name() ) )
                        .findFirst()
                        .orElseThrow( () -> new GenericRuntimeException( "Preset '%s' of adapter %s references the unknown setting '%s'.", preset.name(), clazz.getSimpleName(), entry.name() ) );
                if ( setting instanceof AbstractAdapterSettingList list && !list.options.contains( entry.value() ) ) {
                    throw new GenericRuntimeException( "Preset '%s' of adapter %s uses the value '%s' for setting '%s', which is not one of its options.", preset.name(), clazz.getSimpleName(), entry.value(), entry.name() );
                }
            }
        }
        return presets;
    }


    public Map<String, String> getDefaultSettings() {
        Map<String, String> map = new HashMap<>();
        for ( AbstractAdapterSetting s : settings ) {
            if ( map.put( s.name, s.defaultValue ) != null ) {
                throw new IllegalStateException( "Duplicate key" );
            }
        }
        return map;
    }


    public DeployMode getDefaultMode() {
        return clazz.getAnnotation( AdapterProperties.class ).defaultMode();
    }

}
