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


import java.math.BigDecimal;
import java.util.Arrays;
import org.polypheny.db.config.exception.ConfigRuntimeException;


/**
 * Representation of a value represented as array. All of the fields in the array need to be of the same type.
 */
public class ConfigArray extends Config {

    private ConfigScalar[] array;
    private ConfigScalar[] defaultArray;


    public ConfigArray( final String key, final int[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigInteger( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    public ConfigArray( final String key, final double[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigDouble( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    public ConfigArray( final String key, final long[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigLong( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    public ConfigArray( final String key, final BigDecimal[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigDecimal( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    public ConfigArray( final String key, final String[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigString( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    public ConfigArray( final String key, final boolean[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i < array.length; i++ ) {
            fill[i] = (ConfigScalar) new ConfigBoolean( key + "." + i, array[i] ).isObservable( false );
        }
        this.array = fill;
        this.defaultArray = this.array.clone();
    }


    @Override
    public Object getPlainValueObject() {
        return array;
    }


    @Override
    public Object getDefaultValue() {
        return defaultArray;
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
        return Arrays.equals( defaultArray, array );
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        array = defaultArray.clone();
    }


    @Override
    public int[] getIntArray() {
        int[] out = new int[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getInt();
        }
        return out;
    }


    @Override
    public boolean setIntArray( int[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setInt( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public double[] getDoubleArray() {
        double[] out = new double[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getDouble();
        }
        return out;
    }


    @Override
    public boolean setDoubleArray( double[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setDouble( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public long[] getLongArray() {
        long[] out = new long[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getLong();
        }
        return out;
    }


    @Override
    public boolean setLongArray( long[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setLong( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public BigDecimal[] getDecimalArray() {
        BigDecimal[] out = new BigDecimal[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getDecimal();
        }
        return out;
    }


    @Override
    public boolean setDecimalArray( BigDecimal[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setDecimal( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }

    }


    @Override
    public String[] getStringArray() {
        String[] out = new String[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getString();
        }
        return out;
    }


    @Override
    public boolean setStringArray( String[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setString( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }

    }


    @Override
    public boolean[] getBooleanArray() {
        boolean[] out = new boolean[array.length];
        for ( int i = 0; i < out.length; i++ ) {
            out[i] = array[i].getBoolean();
        }
        return out;
    }


    @Override
    public boolean setBooleanArray( boolean[] value ) {
        if ( validate( value ) ) {
            int counter = 0;
            for ( ConfigScalar c : array ) {
                c.setBoolean( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        throw new ConfigRuntimeException( "Reading arrays of values from config files is not supported yet." );
    }


    @Override
    public boolean parseStringAndSetValue( String valueStr ) {
        throw new ConfigRuntimeException( "Parse and set is not implemented for this type." );
    }

}
