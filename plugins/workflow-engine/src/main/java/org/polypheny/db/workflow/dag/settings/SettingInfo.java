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

package org.polypheny.db.workflow.dag.settings;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.workflow.dag.annotations.StringSetting;

@Value
@NonFinal
public abstract class SettingInfo {

    public static String DEFAULT_GROUP = "";
    public static String ADVANCED_GROUP = "advanced";
    public static String DEFAULT_SUBGROUP = "";

    SettingType type;
    String key;
    String displayName;
    String description;
    SettingValue defaultValue;

    String group;
    String subgroup;
    int position;
    String subOf;


    public SettingInfo( SettingType type, String key, String displayName, String description, SettingValue defaultValue, String group, String subgroup, int position, String subOf ) {
        this.type = type;
        this.key = key;
        this.displayName = displayName;
        this.description = description;
        this.defaultValue = defaultValue;
        this.group = group;
        this.subgroup = subgroup;
        this.position = position;
        this.subOf = subOf;
    }


    public SettingInfo( SettingType type, String key, String displayName, String description, SettingValue defaultValue, int position, String subOf ) {
        this( type, key, displayName, description, defaultValue, DEFAULT_GROUP, DEFAULT_SUBGROUP, position, subOf );
    }


    public static List<SettingInfo> fromAnnotations( Annotation[] annotations ) {
        List<SettingInfo> settings = new ArrayList<>();

        for ( Annotation annotation : annotations ) {
            if ( annotation instanceof StringSetting a ) {
                settings.add( StringSettingInfo.fromAnnotation( a ) );
            } else if ( annotation instanceof StringSetting.List a ) {
                Arrays.stream( a.value() ).forEach( el -> settings.add( StringSettingInfo.fromAnnotation( el ) ) );
            } else {
                throw new NotImplementedException( "Setting type is not yet implemented" );
            }

        }
        return settings;
    }


    public enum SettingType {
        STRING,
    }

}
