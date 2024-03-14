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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.typesafe.config.ConfigException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.ConfigClazz.ClassesAdapter;
import org.polypheny.db.config.exception.ConfigRuntimeException;
import org.reflections.Reflections;


@Slf4j
public class ConfigClazzList extends Config {

    @JsonAdapter(ClassesAdapter.class)
    @SerializedName("values")
    private final Set<Class> classes;
    @JsonAdapter(ValueAdapter.class)
    private final List<Class> value;
    private List<Class> oldValue;
    private List<Class> defaultValue;


    public ConfigClazzList( final String key, final Class superClass ) {
        super( key );
        Reflections reflections = new Reflections( "org.polypheny.db" );
        //noinspection unchecked
        classes = ImmutableSet.copyOf( reflections.getSubTypesOf( superClass ) );
        this.value = new ArrayList<>();
        this.defaultValue = new ArrayList<>();
        this.webUiFormType = WebUiFormType.CHECKBOXES;
    }


    public ConfigClazzList( final String key, final Class superClass, boolean activateAll ) {
        this( key, superClass );
        if ( activateAll ) {
            setClazzList( new ArrayList<>( this.classes ) );
        }
    }


    public ConfigClazzList( final String key, final Class superClass, final List<Class> defaultValue ) {
        this( key, superClass );
        setClazzList( defaultValue );
        this.defaultValue = ImmutableList.copyOf( defaultValue );
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
        return defaultValue.containsAll( value ) && value.containsAll( defaultValue );
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use:  {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        setClazzList( ImmutableList.copyOf( defaultValue ) );
    }


    @Override
    public Set<Class> getClazzes() {
        return classes;
    }


    @Override
    public List<Class> getClazzList() {
        return ImmutableList.copyOf( value );
    }


    @Override
    public boolean setClazzList( final List<Class> value ) {
        if ( classes.containsAll( value ) ) {
            if ( validate( value ) ) {
                if ( requiresRestart() ) {
                    if ( this.oldValue == null ) {
                        this.oldValue = new ArrayList<>();
                        this.oldValue.addAll( this.value );
                    }
                }
                this.value.clear();
                this.value.addAll( value );
                if ( this.oldValue != null && this.oldValue.equals( this.value ) ) {
                    this.oldValue = null;
                }
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This list contains at least one class that does not implement the specified super class!" );
        }
    }


    @Override
    public boolean addClazz( final Class value ) {
        if ( classes.contains( value ) ) {
            if ( validate( value ) ) {
                this.value.add( value );
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This class does not implement the specified super class!" );
        }
    }


    @Override
    public boolean removeClazz( final Class value ) {
        boolean b = this.value.remove( value );
        notifyConfigListeners();
        return b;
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        final List<String> value;
        try {
            value = conf.getStringList( this.getKey() ); // read value from config file
            this.value.clear();
            for ( String v : value ) {
                addClazz( getByString( v ) );
            }
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        Gson gson = new Gson();
        List<String> val = gson.fromJson( value, List.class );
        List<Class> toAdd = new ArrayList<>();
        for ( Class c : classes ) {
            if ( val.contains( c.getName() ) ) {
                toAdd.add( c );
            }
        }
        return this.setClazzList( toAdd );
    }


    private Class getByString( String str ) throws ConfigRuntimeException {
        for ( Class c : classes ) {
            if ( str.equalsIgnoreCase( c.getName() ) ) {
                return c;
            }
        }
        throw new ConfigRuntimeException( "No class with name \"" + str + "\" found in the set of valid classes." );
    }


    static class ValueAdapter extends TypeAdapter<List<Class>> {

        @Override
        public void write( final JsonWriter out, final List<Class> classes ) throws IOException {
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
        public List<Class> read( final JsonReader in ) throws IOException {
            List<Class> list = new ArrayList<>();
            in.beginArray();
            while ( in.hasNext() ) {
                try {
                    Class c = Class.forName( in.nextString() );
                    list.add( c );
                } catch ( ClassNotFoundException e ) {
                    log.error( "Caught exception!", e );
                    list.add( null );
                }
            }
            in.endArray();
            return ImmutableList.copyOf( list );
        }

    }


}
