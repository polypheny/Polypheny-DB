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
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.config.exception.ConfigRuntimeException;

public class ConfigPolicy extends ConfigObject{

    @Getter
    @Setter
    String category;

    @Getter
    @Setter
    String selectedFor;

    public ConfigPolicy( String category, String selectedFor){
        this(idBuilder.getAndIncrement(), category, selectedFor);
    }

    public ConfigPolicy(int id, String category, String selectedFor ) {
        super( "policyConfig" + id );
        this.id = id;
        this.category = category;
        this.selectedFor = selectedFor;

        this.webUiFormType = WebUiFormType.POLICY;

    }

    public static ConfigPolicy fromMap( Map<String, Object> value ) {
        ConfigPolicy config = new ConfigPolicy(
                (String) value.get( "category" ),
                (String) value.get( "selectedFor" )
        );
        return config;
    }

    public Map<String, String> getSettings() {
        Map<String, String> settings = new HashMap<>();

        settings.put( "category", category );
        settings.put( "selectedFor", selectedFor );

        return settings;
    }

    public static Map<String, Object> parseConfigToMap( com.typesafe.config.Config conf ) {
        Map<String, Object> confMap = new HashMap<>();

        confMap.put( "category", conf.getString( "category" ) );
        confMap.put( "selectedFor", conf.getString( "selectedFor" ) );
        return confMap;
    }

    @Override
    public Object getPlainValueObject() {
        throw new ConfigRuntimeException( "Not supported for Policy Configs" );
    }


    @Override
    public Object getDefaultValue() {
        throw new ConfigRuntimeException( "Not supported for Policy Configs" );
    }


    @Override
    public boolean isDefault() {
        throw new ConfigRuntimeException( "Not supported for Policy Configs" );
    }


    @Override
    public void resetToDefault() {
        throw new ConfigRuntimeException( "Not supported for Policy Configs" );
    }


    @Override
    void setValueFromFile( Config conf ) {
        fromMap( parseConfigToMap( conf ) );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return false;
    }

}
