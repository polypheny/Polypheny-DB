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
import com.typesafe.config.ConfigException;
import org.polypheny.db.config.exception.ConfigRuntimeException;


public class ConfigBoolean extends ConfigScalar {

    private boolean value;
    private Boolean oldValue;
    private boolean defaultValue;


    public ConfigBoolean( final String key, final boolean value ) {
        super( key );
        this.webUiFormType = WebUiFormType.BOOLEAN;
        this.value = value;
        this.defaultValue = value;
    }


    public ConfigBoolean( final String key, final String description, final boolean value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.BOOLEAN;
        this.value = value;
        this.defaultValue = value;
    }


    @Override
    public boolean getBoolean() {
        return this.value;
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
        return defaultValue == value;
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        setBoolean( defaultValue );
    }


    @Override
    public boolean setBoolean( final boolean b ) {
        if ( validate( value ) ) {
            if ( requiresRestart() ) {
                if ( this.oldValue == null ) {
                    this.oldValue = this.value;
                }
            }

            this.value = b;
            if ( this.oldValue != null && this.oldValue.equals( value ) ) {
                this.oldValue = null;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }

    }


    @Override
    void setValueFromFile( final Config conf ) {
        final boolean value;
        try {
            value = conf.getBoolean( this.getKey() ); // read value from config file
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
        setBoolean( value );
    }


    @Override
    public boolean parseStringAndSetValue( String valueStr ) {
        return setBoolean( Boolean.parseBoolean( valueStr ) );
    }

}
