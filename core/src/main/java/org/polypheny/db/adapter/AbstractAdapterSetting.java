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

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingBoolean;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;

@Accessors(chain = true)
public abstract class AbstractAdapterSetting {

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
    public final List<DeploySetting> appliesTo;


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
