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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.models.ActivityModel;
import org.reflections.Reflections;

public class ActivityRegistry {

    private static final Map<String, ActivityInfo> registry;


    static {
        registry = registerActivities();
    }


    public static ActivityInfo get( String activityType ) {
        return registry.get( activityType );
    }


    public static Activity fromModel( ActivityModel model ) {
        ActivityInfo info = get( model.getType() );
        if ( info == null ) {
            throw new IllegalArgumentException( "No activity found for type: " + model.getType() );
        }
        try {
            Constructor<? extends Activity> constructor = info.getActivityClass().getConstructor( ActivityModel.class );
            return constructor.newInstance( model );
        } catch ( InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e ) {
            throw new RuntimeException( "Encountered problem during instantiation for type: " + model.getType() );
        }
    }

    public static String serialize() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString( registry );
        } catch ( JsonProcessingException e ) {
            throw new RuntimeException( e );
        }
    }


    private static Map<String, ActivityInfo> registerActivities() {
        Map<String, ActivityInfo> registry = new HashMap<>();
        for ( Class<? extends Activity> cls : findAllAnnotatedActivities() ) {
            ActivityDefinition definition = cls.getAnnotation( ActivityDefinition.class );
            String key = definition.type();
            assert !registry.containsKey( key ) : "Found duplicate activity type: " + key;

            ActivityInfo info = ActivityInfo.fromAnnotations( cls, definition );
            registry.put( key, info );
        }
        return Collections.unmodifiableMap( registry );
    }


    public static Set<Class<? extends Activity>> findAllAnnotatedActivities() {
        Reflections reflections = new Reflections( "org.polypheny.db.workflow" );
        Set<Class<? extends Activity>> activityClasses = reflections.getSubTypesOf( Activity.class );

        return activityClasses.stream()
                .filter( cls -> cls.isAnnotationPresent( ActivityDefinition.class ) )
                .collect( Collectors.toSet() );
    }

}
