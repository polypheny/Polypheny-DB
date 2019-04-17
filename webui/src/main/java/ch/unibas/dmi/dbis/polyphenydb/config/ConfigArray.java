/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.config;


import java.math.BigDecimal;


/** ConfigArray contains an array of ConfigScalars. All of them need to be of the same type. */
public class ConfigArray extends Config {

    private ConfigScalar[] array;


    public ConfigArray ( final String key, final int[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigInteger( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
    }


    public ConfigArray ( final String key, final double[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigDouble( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
    }


    public ConfigArray ( final String key, final long[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigLong( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
    }


    public ConfigArray ( final String key, final BigDecimal[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigDecimal( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
    }


    public ConfigArray ( final String key, final String[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigString( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
    }


    public ConfigArray ( final String key, final boolean[] array ) {
        super( key );
        ConfigScalar[] fill = new ConfigScalar[array.length];
        for ( int i = 0; i<array.length; i++) {
            fill[i] = (ConfigScalar) new ConfigBoolean( key+"."+i, array[i] ).isObservable( false );
        }
        this.array = fill;
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
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
        if( validate ( value )) {
            int counter = 0;
            for( ConfigScalar c : array ) {
                c.setBoolean( value[counter] );
                counter++;
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


}
