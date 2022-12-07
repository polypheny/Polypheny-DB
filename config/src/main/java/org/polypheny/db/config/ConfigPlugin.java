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

import com.typesafe.config.Config;
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


    public ConfigPlugin( String pluginId, PluginStatus status, String imageUrl, List<String> categories, String description ) {
        super( "pluginConfig" + pluginId, description );
        this.pluginId = pluginId;
        this.status = status;
        this.imageUrl = imageUrl;
        this.categories = categories;

        this.webUiFormType = WebUiFormType.PLUGIN_INSTANCE;
    }


    public static ConfigScalar fromMap( Map<String, Object> value ) {

        return new ConfigPlugin(
                (String) value.get( "pluginId" ),
                PluginStatus.valueOf( (String) value.get( "status" ) ),
                (String) value.get( "imageUrl" ),
                mapCategories( (String) value.get( "categories" ) ),
                (String) value.get( "description" ) );
    }


    private static List<String> mapCategories( String categories ) {
        return List.of( categories.trim().replace( "[", "" ).replace( "]", "" ).split( "," ) );
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
        // todo dl
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        throw new ConfigRuntimeException( "Not supported for Plugin Configs" );
        // todo dl
    }

}
