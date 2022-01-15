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
import java.math.BigDecimal;
import org.polypheny.db.config.exception.ConfigRuntimeException;


public class ConfigDecimal extends ConfigScalar {

    private BigDecimal value;
    private BigDecimal oldValue;
    private BigDecimal defaultValue;


    public ConfigDecimal( final String key, final BigDecimal value ) {
        super( key );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
        this.defaultValue = this.value;
    }


    public ConfigDecimal( final String key, final String description, final BigDecimal value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
        this.defaultValue = this.value;
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
     * @return true if it is set to default, false if it deviates.
     */
    @Override
    public boolean isDefault() {
        return value == defaultValue;
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        setDecimal( defaultValue );
    }


    @Override
    public BigDecimal getDecimal() {
        return this.value;
    }


    @Override
    public boolean setDecimal( final BigDecimal value ) {
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
    }


    @Override
    void setValueFromFile( final Config conf ) {
        final String stringValue;
        try {
            stringValue = conf.getString( this.getKey() ); // read value from config file
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
        setDecimal( new BigDecimal( stringValue ) );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return setDecimal( new BigDecimal( value ) );
    }

}
