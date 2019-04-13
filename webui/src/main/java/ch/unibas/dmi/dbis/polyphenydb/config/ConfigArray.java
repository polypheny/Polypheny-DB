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


    public ConfigArray( final String key, final ConfigScalar[] array ) {
        super( key );
        this.array = array;
    }


    public ConfigArray( final String key, final String description, final ConfigScalar[] array ) {
        super( key, description );
        this.array = array;
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
    public void setIntArray( int[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setInt( value[counter] );
            counter++;
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
    public void setDoubleArray( double[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setDouble( value[counter] );
            counter++;
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
    public void setLongArray( long[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setLong( value[counter] );
            counter++;
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
    public void setDecimalArray( BigDecimal[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setDecimal( value[counter] );
            counter++;
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
    public void setStringArray( String[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setString( value[counter] );
            counter++;
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
    public void setBooleanArray( boolean[] value ) {
        int counter = 0;
        for( ConfigScalar c : array ) {
            c.setBoolean( value[counter] );
            counter++;
        }
    }


}
