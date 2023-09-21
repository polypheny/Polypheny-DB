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

package org.polypheny.db.adapter.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.AbstractAdapterSettingList;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.Function4;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.DockerManager;

@Value
public class AdapterTemplate {

    @Getter
    public Map<String, String> defaultSettings;

    public Class<?> clazz;
    public String adapterName;
    public AdapterType adapterType;
    Function4<Long, String, Map<String, String>, Adapter<?>> deployer;
    public List<AbstractAdapterSetting> settings;
    public List<DeployMode> modes;
    public long id;
    public String description;


    public AdapterTemplate( long id, Class<?> clazz, String adapterName, List<AbstractAdapterSetting> settings, Map<String, String> defaultSettings, List<DeployMode> modes, String description, Function4<Long, String, Map<String, String>, Adapter<?>> deployer ) {
        this.id = id;
        this.adapterName = adapterName;
        this.description = description;
        this.clazz = clazz;
        this.settings = settings;
        this.defaultSettings = defaultSettings;
        this.modes = modes;
        this.adapterType = getAdapterType( clazz );
        this.deployer = deployer;
    }


    public static AdapterType getAdapterType( Class<?> clazz ) {
        return DataStore.class.isAssignableFrom( clazz ) ? AdapterType.STORE : AdapterType.SOURCE;
    }


    public static AdapterTemplate fromString( String adapterName, AdapterType adapterType ) {
        return AdapterManager.getAdapterType( adapterName, adapterType );
    }


    public static List<AbstractAdapterSetting> getAllSettings( Class<? extends Adapter<?>> clazz, Map<String, String> defaultSettings ) {
        AdapterProperties properties = clazz.getAnnotation( AdapterProperties.class );
        if ( clazz.getAnnotation( AdapterProperties.class ) == null ) {
            throw new GenericRuntimeException( "The used adapter does not annotate its properties correctly." );
        }
        List<AbstractAdapterSetting> settings = new ArrayList<>( AbstractAdapterSetting.fromAnnotations( clazz.getAnnotations(), properties ) );
        if ( Arrays.stream( properties.usedModes() ).anyMatch( m -> m == DeployMode.DOCKER ) ) {
            String instanceId = DockerManager.getInstance().getDockerInstances().keySet().stream().findFirst().orElseThrow().toString();
            settings.add( new AbstractAdapterSettingList( "instanceId", false, null, true, false, Arrays.stream( properties.usedModes() ).map( DeployMode::getName ).collect( Collectors.toList() ), List.of( DeploySetting.DOCKER ), instanceId, 0 ) );
        }
        return settings;
    }

}
