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

package org.polypheny.db.workflow.dag.activities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.AdvancedGroup;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.Group;
import org.polypheny.db.workflow.dag.settings.SettingDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class ActivityRegistry {

    private static final String ACTIVITY_PATH = "org.polypheny.db.workflow.dag.activities.impl";

    @Getter
    private static final Map<String, ActivityDef> registry;


    static {
        registry = registerActivities();
    }


    /**
     * Returns the ActivityDef for the specified type or throws an IllegalArgumentException if it does not exist.
     *
     * @param activityType the unique identifier for the activityType.
     * @return the ActivityDef corresponding to activityType
     * @throws IllegalArgumentException if the activityType is not registered
     */
    public static ActivityDef get( String activityType ) {
        ActivityDef def = registry.get( activityType );
        if ( def == null ) {
            throw new IllegalArgumentException( "No activity found for type: " + activityType );
        }
        return def;
    }


    public static Activity activityFromType( String activityType ) {
        ActivityDef def = get( activityType );
        try {
            Constructor<? extends Activity> constructor = def.getActivityClass().getConstructor();
            return constructor.newInstance();
        } catch ( InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e ) {
            throw new RuntimeException( "Encountered problem during activity instantiation for type: " + activityType );
        }
    }


    /**
     * Builds a map of setting values for a specified activity type based on the resolved JSON nodes (meaning it contains no variable references).
     *
     * @param activityType the unique identifier for the activity type.
     * @param resolved a map of setting keys to their resolved JSON node representations.
     * @return an unmodifiable map of setting keys to their corresponding {@code SettingValue} instances.
     * @throws IllegalArgumentException if no activity definition is found for the provided {@code activityType}
     * or if a JsonNode has an unexpected format.
     * @throws InvalidSettingException if the settingValue is not valid for the corresponding SettingDef
     */
    public static Settings buildSettingValues( String activityType, Map<String, JsonNode> resolved ) throws InvalidSettingException {
        return buildSettingValues( activityType, resolved, false );
    }


    public static Settings buildSettingValues( String activityType, Map<String, JsonNode> resolved, boolean disableValidation ) throws InvalidSettingException {
        Map<String, SettingDef> settingDefs = get( activityType ).getSettings();

        Map<String, SettingValue> settingValues = new HashMap<>();
        for ( Entry<String, JsonNode> entry : resolved.entrySet() ) {
            String key = entry.getKey();
            SettingValue settingValue = disableValidation ?
                    settingDefs.get( key ).buildValue( entry.getValue() ) :
                    settingDefs.get( key ).buildValidatedValue( entry.getValue() );
            settingValues.put( key, settingValue );
        }
        return new Settings( settingValues );
    }


    /**
     * Builds a map of available setting values for the specified activity type using resolved JSON nodes.
     * If a supplied setting is {@link Optional#empty()}, it is represented as {@link Optional#empty()} in the output map.
     *
     * @param activityType the identifier for the activity type.
     * @param resolved a map of setting keys to {@link Optional<JsonNode>} values, where unresolved settings are {@link Optional#empty()}.
     * @return a wrapper around a map of setting keys to {@link Optional<SettingValue>} instances, where missing or unresolved settings are {@link Optional#empty()}.
     * @throws IllegalArgumentException if the {@code activityType} is invalid or a {@link JsonNode} has an unexpected format or is invalid.
     */
    public static SettingsPreview buildAvailableSettingValues( String activityType, Map<String, Optional<JsonNode>> resolved ) throws InvalidSettingException {
        Map<String, SettingDef> settingDefs = get( activityType ).getSettings();

        Map<String, Optional<SettingValue>> settingValues = new HashMap<>();
        for ( Entry<String, Optional<JsonNode>> entry : resolved.entrySet() ) {
            String key = entry.getKey();
            Optional<JsonNode> node = entry.getValue();
            if ( node.isPresent() ) {
                settingValues.put( key, Optional.of( settingDefs.get( key ).buildValidatedValue( node.get() ) ) );
            } else {
                settingValues.put( key, Optional.empty() );
            }
        }
        return new SettingsPreview( settingValues );
    }


    /**
     * Get the default setting values for the specified activityType.
     *
     * @param activityType the identifier for the activity type.
     * @return an unmodifiable map of setting keys to default {@link SettingValue}s for that key.
     */
    public static Settings getDefaultSettings( String activityType ) {
        Map<String, SettingDef> settingDefs = get( activityType ).getSettings();
        Map<String, SettingValue> settingValues = new HashMap<>();

        for ( Entry<String, SettingDef> entry : settingDefs.entrySet() ) {
            settingValues.put( entry.getKey(), entry.getValue().getDefaultValue() );
        }
        return new Settings( settingValues );
    }


    /**
     * Get the default setting values for the specified activityType in its serializable form.
     *
     * @param activityType the identifier for the activity type.
     * @return an unmodifiable map of setting keys to their default values.
     */
    public static Map<String, JsonNode> getSerializableDefaultSettings( String activityType ) {
        return getDefaultSettings( activityType ).getSerializableSettings();
    }


    public static String serialize() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString( registry );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }


    private static Map<String, ActivityDef> registerActivities() {
        Map<String, ActivityDef> registry = new HashMap<>();
        for ( Class<? extends Activity> cls : findAllAnnotatedActivities() ) {
            ActivityDefinition definition = cls.getAnnotation( ActivityDefinition.class );
            String key = definition.type();
            assert !registry.containsKey( key ) : "Found duplicate activity type: " + key;

            Annotation groups = cls.getAnnotation( Group.class );
            if ( groups == null ) {
                groups = cls.getAnnotation( Group.List.class );
            }

            ActivityDef def = ActivityDef.fromAnnotations(
                    cls,
                    definition,
                    groups,
                    cls.getAnnotation( DefaultGroup.class ),
                    cls.getAnnotation( AdvancedGroup.class ),
                    cls.getAnnotations() );
            registry.put( key, def );
        }
        return Collections.unmodifiableMap( registry );
    }


    public static Set<Class<? extends Activity>> findAllAnnotatedActivities() {
        final ClassLoader loader = PolySerializable.CLASS_LOADER;

        Reflections reflections = new Reflections(
                new ConfigurationBuilder()
                        .addClassLoaders( loader )
                        .forPackages( ACTIVITY_PATH )
                        .setUrls( ClasspathHelper.forClassLoader( loader ).stream().filter( u -> u.getPath().contains( "workflow-engine" ) ).toList() )
        );
        Set<Class<? extends Activity>> activityClasses = reflections.getSubTypesOf( Activity.class );

        return activityClasses.stream()
                .filter( cls -> cls.isAnnotationPresent( ActivityDefinition.class ) )
                .collect( Collectors.toSet() );
    }

}
