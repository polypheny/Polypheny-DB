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

package org.polypheny.db.config;


import com.google.common.collect.ImmutableSet;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.typesafe.config.ConfigException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.exception.ConfigRuntimeException;
import org.reflections.Reflections;


@Slf4j
public class ConfigClazz extends Config {

    @JsonAdapter(ClassesAdapter.class)
    @SerializedName("values")
    private final Set<Class> classes;
    @JsonAdapter(ValueAdapter.class)
    private Class value;
    private Class oldValue;
    private String defaultValue;  // Purposely set to string because clone() is not available for class. String is okay since only link to class is necessary. Selection of classes is restricted anyway thanks to {@limk #classes}


    public ConfigClazz( final String key, final Class superClass, final Class defaultValue ) {
        super( key );
        Reflections reflections = new Reflections( "org.polypheny.db" );
        //noinspection unchecked
        classes = ImmutableSet.copyOf( reflections.getSubTypesOf( superClass ) );
        setClazz( defaultValue );
        this.defaultValue = defaultValue.toString();
        this.webUiFormType = WebUiFormType.SELECT;
    }


    @Override
    public Object getPlainValueObject() {
        return value;
    }


    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }


    /**
     * Checks if the currently set config value, is equal to the system configured default.
     * If you want to reset it to the configured defaultValue use {@link #resetToDefault()}.
     * To change the systems default value you can use: {@link #changeDefaultValue(Object)}.
     *
     * @return true if it is set to default, false if it deviates
     */
    @Override
    public boolean isDefault() {
        return defaultValue.equals( value.toString() );
    }


    @Override
    public void resetToDefault() {
        parseStringAndSetValue( defaultValue );
    }


    @Override
    public Set<Class> getClazzes() {
        return classes;
    }


    @Override
    public Class getClazz() {
        return value;
    }


    @Override
    public boolean setClazz( final Class value ) {
        if ( classes.contains( value ) ) {
            if ( validate( value ) ) {
                if ( requiresRestart() ) {
                    if ( this.oldValue == null ) {
                        this.oldValue = this.value;
                    }
                }
                this.value = value;
                if ( this.oldValue != null && this.oldValue.equals( this.value ) ) {
                    this.oldValue = null;
                }
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This class does not implement the specified super class" );
        }
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        try {
            setClazz( getByString( conf.getString( this.getKey() ) ) );
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
    }


    @Override
    public boolean parseStringAndSetValue( String valueStr ) {
        return setClazz( getByString( valueStr ) );
    }


    private Class getByString( String str ) throws ConfigRuntimeException {
        for ( Class c : classes ) {
            if ( str.equalsIgnoreCase( c.getName() ) ) {
                return c;
            }
        }
        throw new ConfigRuntimeException( "No class with name " + str + " found in the list of valid classes." );
    }


    class ClassesAdapter extends TypeAdapter<Set<Class>> {

        @Override
        public void write( final JsonWriter out, final Set<Class> classes ) throws IOException {
            if ( classes == null ) {
                out.nullValue();
                return;
            }
            out.beginArray();
            for ( Class c : classes ) {
                out.value( c.getName() );
            }
            out.endArray();
        }


        @Override
        public Set<Class> read( final JsonReader in ) throws IOException {
            Set<Class> set = new HashSet<>();
            in.beginArray();
            while ( in.hasNext() ) {
                try {
                    Class c = Class.forName( in.nextString() );
                    set.add( c );
                } catch ( ClassNotFoundException e ) {
                    log.error( "Caught exception!", e );
                    set.add( null );
                }
            }
            in.endArray();
            return ImmutableSet.copyOf( set );
        }

    }


    static class ValueAdapter extends TypeAdapter<Class> {

        @Override
        public void write( final JsonWriter out, final Class value ) throws IOException {
            if ( value == null ) {
                out.nullValue();
                return;
            }
            out.value( value.getName() );
        }


        @Override
        public Class read( final JsonReader in ) throws IOException {
            try {
                return Class.forName( in.nextString() );
            } catch ( ClassNotFoundException e ) {
                log.error( "Caught exception!", e );
                return null;
            }
        }

    }

}
