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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.typesafe.config.ConfigException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.config.exception.ConfigRuntimeException;


public class ConfigEnumList extends Config {

    @SerializedName("values")
    private final Set<Enum> enumValues;
    private final List<Enum> value;
    private List<Enum> oldValue;
    private List<Enum> defaultValue;


    public ConfigEnumList( final String key, final String description, final Class enumClass ) {
        super( key, description );
        //noinspection unchecked
        enumValues = ImmutableSet.copyOf( EnumSet.allOf( enumClass ) );
        this.value = new ArrayList<>();
        this.defaultValue = new ArrayList<>();
        this.webUiFormType = WebUiFormType.CHECKBOXES;
    }


    public ConfigEnumList( final String key, final String description, final Class superClass, final List<Enum> defaultValue ) {
        this( key, description, superClass );
        setEnumList( defaultValue );
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
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        setEnumList( ImmutableList.copyOf( defaultValue ) );
    }


    @Override
    public Set<Enum> getEnumValues() {
        return enumValues;
    }


    @Override
    public List<Enum> getEnumList() {
        return ImmutableList.copyOf( value );
    }


    @Override
    public boolean setEnumList( final List<Enum> value ) {
        if ( enumValues.containsAll( value ) ) {
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
            throw new ConfigRuntimeException( "This list contains at least one enum that does not belong to the defined enum class!" );
        }
    }


    @Override
    public boolean addEnum( final Enum value ) {
        if ( enumValues.contains( value ) ) {
            if ( validate( value ) ) {
                this.value.add( value );
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This enum does not belong to the specified enum class!" );
        }
    }


    @Override
    public boolean removeEnum( final Enum value ) {
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
                addEnum( getByString( v ) );
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
        ArrayList<String> val = gson.fromJson( value, ArrayList.class );
        List<Enum> toAdd = new ArrayList<>();
        for ( Enum e : enumValues ) {
            if ( val.contains( e.name() ) ) {
                toAdd.add( e );
            }
        }
        return this.setEnumList( toAdd );
    }


    private Enum getByString( String str ) throws ConfigRuntimeException {
        for ( Enum e : enumValues ) {
            if ( str.equalsIgnoreCase( e.name() ) ) {
                return e;
            }
        }
        throw new ConfigRuntimeException( "No enum with name \"" + str + "\" found in the set of valid enums." );
    }

}
