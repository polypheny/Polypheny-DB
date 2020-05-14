/*
 * Copyright 2019-2020 The Polypheny Project
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

    boolean value;


    public ConfigBoolean( final String key, final boolean value ) {
        super( key );
        this.webUiFormType = WebUiFormType.BOOLEAN;
        this.value = value;
    }


    public ConfigBoolean( final String key, final String description, final boolean value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.BOOLEAN;
        this.value = value;
    }


    @Override
    public boolean getBoolean() {
        return this.value;
    }


    @Override
    public boolean setBoolean( final boolean b ) {
        if ( validate( value ) ) {
            this.value = b;
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
