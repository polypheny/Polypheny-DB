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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.util.Wrapper;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;

@Getter
public abstract class SettingDef {

    public static String SUB_SEP = ">"; // this separator is used to specify dependencies on values of other settings. For example "modeSelector>mode1" to specify that this setting is only active if modeSelector is equal to mode1

    private final SettingType type;
    private final String key;
    private final String displayName;
    private final String description;

    @JsonIgnore // the UI does not handle defaultValues. This is instead done by the backend, at the moment the activity is created.
    private final SettingValue defaultValue;

    private final String group;
    private final String subgroup;
    private final int position;
    private final String subOf;


    public SettingDef( SettingType type, String key, String displayName, String description, SettingValue defaultValue, String group, String subgroup, int position, String subOf ) {
        assert !key.contains( SettingDef.SUB_SEP ) : "Setting key must not contain separator symbol '" + SUB_SEP + "': " + key;
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


    /**
     * Builds a {@link SettingValue} from the given {@link JsonNode} that represents a value for this SettingInfo.
     *
     * @param node the {@link JsonNode} containing the data (with any variable references already replaced) to build the {@link SettingValue}.
     * @return the constructed {@link SettingValue}.
     * @throws IllegalArgumentException if the {@link JsonNode} has an unexpected format.
     */
    public abstract SettingValue buildValue( JsonNode node );


    public static List<SettingDef> fromAnnotations( Annotation[] annotations ) {
        List<SettingDef> settings = new ArrayList<>();

        for ( Annotation annotation : annotations ) {
            if ( annotation instanceof ActivityDefinition ) {
                continue; // activity definition is treated separately
            }

            // Register setting annotations here
            if ( annotation instanceof StringSetting a ) {
                settings.add( new StringSettingDef( a ) );
            } else if ( annotation instanceof StringSetting.List a ) {
                Arrays.stream( a.value() ).forEach( el -> settings.add( new StringSettingDef( el ) ) );
            } else if ( annotation instanceof IntSetting a ) {
                settings.add( new IntSettingDef( a ) );
            } else if ( annotation instanceof IntSetting.List a ) {
                Arrays.stream( a.value() ).forEach( el -> settings.add( new IntSettingDef( el ) ) );
            }

        }
        return settings;
    }


    public enum SettingType {
        STRING,
        INT
    }


    public interface SettingValue extends Wrapper {

        JsonNode toJson( JsonMapper mapper );

    }

}
