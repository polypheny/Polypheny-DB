/*
 * Copyright 2019-2021 The Polypheny Project
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


public class ConfigInteger extends ConfigScalar {

    private int value;
    private int defaultValue;


    public ConfigInteger( final String key, final int value ) {
        super( key );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
        this.defaultValue = value;
    }


    public ConfigInteger( final String key, final String description, final int value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
        this.defaultValue = value;
    }


    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }


    @Override
    public int getInt() {
        return this.value;
    }


    @Override
    public boolean setInt( final int value ) {
        if ( validate( value ) ) {
            this.value = value;
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public long getLong() {
        return (long) this.value;
    }


    @Override
    void setValueFromFile( final Config conf ) {
        final int value;
        try {
            value = conf.getInt( this.getKey() ); // read value from config file
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }
        setInt( value );
    }


    @Override
    public boolean parseStringAndSetValue( String value ) {
        return setInt( Integer.parseInt( value ) );
    }

}
