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

package org.polypheny.db.config;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.config.exception.ConfigRuntimeException;

public class ConfigPlugin extends ConfigObject {

    @Getter
    private final String pluginId;
    @Getter
    private final PluginStatus status;
    @Getter
    private final String imageUrl;
    private final List<String> categories;
    private final String version;
    private final boolean isSystemComponent;
    private final boolean isUiVisible;


    public ConfigPlugin( String pluginId, PluginStatus status, String imageUrl, List<String> categories, String description, String version, boolean isSystemComponent, boolean isUiVisible ) {
        super( pluginId + "_" + version.replace( ".", "_" ), description );
        this.pluginId = pluginId;
        this.status = status;
        this.imageUrl = imageUrl;
        this.categories = categories;
        this.version = version;
        this.isSystemComponent = isSystemComponent;
        this.isUiVisible = isUiVisible;

        this.webUiFormType = WebUiFormType.PLUGIN_INSTANCE;
    }


    public static ConfigScalar fromMap( Map<String, Object> value ) {

        return new ConfigPlugin(
                (String) value.get( "pluginId" ),
                PluginStatus.valueOf( (String) value.get( "status" ) ),
                (String) value.get( "imageUrl" ),
                (List<String>) value.get( "categories" ),
                (String) value.get( "description" ),
                (String) value.get( "version" ),
                (boolean) value.get( "isSystemComponent" ),
                (boolean) value.get( "isUiVisible" ) );
    }


    public static Object parseConfigToMap( Config conf ) {
        Map<String, Object> confMap = new HashMap<>();

        confMap.put( "pluginId", conf.getString( "pluginId" ) );
        confMap.put( "status", conf.getString( "status" ) );
        confMap.put( "imageUrl", conf.hasPath( "imageUrl" ) ? conf.getString( "imageUrl" ) : null );
        confMap.put( "categories", conf.getStringList( "categories" ) );
        confMap.put( "description", conf.getString( "description" ) );
        confMap.put( "version", conf.getString( "version" ) );
        confMap.put( "isSystemComponent", conf.getBoolean( "isSystemComponent" ) );
        confMap.put( "isUiVisible", conf.getBoolean( "isUiVisible" ) );

        return confMap;
    }


    @Override
    public Object getPlainValueObject() {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    @Override
    public Object getDefaultValue() {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    @Override
    public boolean isDefault() {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    @Override
    public void resetToDefault() {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    @Override
    void setValueFromFile( Config conf ) {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
    }


    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<>();

        map.put( "pluginId", pluginId );
        map.put( "status", status.toString() );
        map.put( "imageUrl", imageUrl );
        map.put( "categories", categories );
        map.put( "description", getDescription() );
        map.put( "version", version );
        map.put( "isSystemComponent", isSystemComponent );
        map.put( "isUiVisible", isUiVisible );

        return map;
    }

}
