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

package org.polypheny.db.adapter;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingBoolean;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;

@Accessors(chain = true)
@Value
@NonFinal
public abstract class AbstractAdapterSetting {

    public String name;
    public boolean canBeNull;
    public String subOf;
    public boolean required;
    public boolean modifiable;
    public String defaultValue;
    public int position;
    @Setter
    @NonFinal
    public String description;
    public AdapterSettingType type;

    @Getter
    public List<DeploySetting> appliesTo;

    public List<String> filenames = new ArrayList<>();


    public AbstractAdapterSetting( final AdapterSettingType type, final String name, final boolean canBeNull, final String subOf, final boolean required, final boolean modifiable, List<DeploySetting> appliesTo, String defaultValue, int position ) {
        this.type = type;
        this.name = name;
        this.canBeNull = canBeNull;
        this.subOf = Objects.equals( subOf, "" ) ? null : subOf;
        this.required = required;
        this.modifiable = modifiable;
        this.position = position;
        this.appliesTo = appliesTo;
        this.defaultValue = defaultValue;
        assert this.subOf == null || this.subOf.split( "_" ).length == 2
                : "SubOf needs to be null or has to be separated by \"_\" and requires link and value due to limitation in Java";
    }


    /**
     * Method generates the correlated AdapterSettings from the provided AdapterAnnotations,
     * Repeatable Annotations are packed inside the underlying Lists of each AdapterSetting
     * as those AdapterSettings belong to a specific adapter the AdapterProperties are used to
     * unpack DeploySettings.ALL to the available modes correctly
     *
     * @param annotations collection of annotations
     * @param properties which are defined by the corresponding Adapter
     * @return a collection containing the available modes and the corresponding collections of AdapterSettings
     */
    public static List<AbstractAdapterSetting> fromAnnotations( Annotation[] annotations, AdapterProperties properties ) {
        List<AbstractAdapterSetting> settings = new ArrayList<>();

        for ( Annotation annotation : annotations ) {
            if ( annotation instanceof AdapterSettingString ) {
                settings.add( AbstractAdapterSettingString.fromAnnotation( (AdapterSettingString) annotation ) );
            } else if ( annotation instanceof AdapterSettingString.List ) {
                Arrays.stream( ((AdapterSettingString.List) annotation).value() ).forEach( el -> settings.add( AbstractAdapterSettingString.fromAnnotation( el ) ) );
            } else if ( annotation instanceof AdapterSettingBoolean ) {
                settings.add( AbstractAdapterSettingBoolean.fromAnnotation( (AdapterSettingBoolean) annotation ) );
            } else if ( annotation instanceof AdapterSettingBoolean.List ) {
                Arrays.stream( ((AdapterSettingBoolean.List) annotation).value() ).forEach( el -> settings.add( AbstractAdapterSettingBoolean.fromAnnotation( el ) ) );
            } else if ( annotation instanceof AdapterSettingInteger ) {
                settings.add( AbstractAdapterSettingInteger.fromAnnotation( (AdapterSettingInteger) annotation ) );
            } else if ( annotation instanceof AdapterSettingInteger.List ) {
                Arrays.stream( ((AdapterSettingInteger.List) annotation).value() ).forEach( el -> settings.add( AbstractAdapterSettingInteger.fromAnnotation( el ) ) );
            } else if ( annotation instanceof AdapterSettingList ) {
                settings.add( AbstractAdapterSettingList.fromAnnotation( (AdapterSettingList) annotation ) );
            } else if ( annotation instanceof AdapterSettingList.List ) {
                Arrays.stream( ((AdapterSettingList.List) annotation).value() ).forEach( el -> settings.add( AbstractAdapterSettingList.fromAnnotation( el ) ) );
            } else if ( annotation instanceof AdapterSettingDirectory ) {
                settings.add( AbstractAdapterSettingDirectory.fromAnnotation( (AdapterSettingDirectory) annotation ) );
            } else if ( annotation instanceof AdapterSettingDirectory.List ) {
                Arrays.stream( ((AdapterSettingDirectory.List) annotation).value() ).forEach( el -> settings.add( AbstractAdapterSettingDirectory.fromAnnotation( el ) ) );
            }
        }
        settings.add( new AbstractAdapterSettingList(
                "mode",
                false,
                null,
                true,
                false,
                Arrays.stream( properties.usedModes() ).map( DeployMode::getName ).toList(),
                List.of( DeploySetting.ALL ),
                properties.defaultMode().getName(),
                0 ) );

        return settings;
    }


    /**
     * In most subclasses, this method returns the defaultValue, because the UI overrides the defaultValue when a new value is set.
     */
    public abstract String getValue();


    public static List<AbstractAdapterSetting> serializeSettings( List<AbstractAdapterSetting> availableSettings, Map<String, String> currentSettings ) {
        List<AbstractAdapterSetting> abstractAdapterSettings = new ArrayList<>();
        for ( AbstractAdapterSetting s : availableSettings ) {
            for ( String current : currentSettings.keySet() ) {
                if ( s.name.equals( current ) ) {
                    abstractAdapterSettings.add( s );
                }
            }
        }
        return abstractAdapterSettings;
    }


    public enum AdapterSettingType {
        DIRECTORY, INTEGER, LIST, STRING, BOOLEAN
    }

}
