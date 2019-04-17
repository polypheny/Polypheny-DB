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


public class ConfigTable extends Config {

    private ConfigScalar[][] table;


    public ConfigTable ( final String key, final int[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigInteger( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable ( final String key, final double[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigDouble( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable ( final String key, final long[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigLong( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable ( final String key, final BigDecimal[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigDecimal( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable ( final String key, final String[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigString( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable ( final String key, final boolean[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i<array.length; i++) {
            for ( int j = 0; j<array[0].length; j++) {
                fill[i][j] = (ConfigScalar) new ConfigBoolean( key+"."+i+"."+j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    @Override
    public int[][] getIntTable() {
        int[][] out = new int[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getInt();
            }
        }
        return out;
    }


    @Override
    public boolean setIntTable( int[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setInt( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public double[][] getDoubleTable() {
        double[][] out = new double[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getDouble();
            }
        }
        return out;
    }


    @Override
    public boolean setDoubleTable( double[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setDouble( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public long[][] getLongTable() {
        long[][] out = new long[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getLong();
            }
        }
        return out;
    }


    @Override
    public boolean setLongTable( long[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setLong( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public BigDecimal[][] getDecimalTable() {
        BigDecimal[][] out = new BigDecimal[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getDecimal();
            }
        }
        return out;
    }


    @Override
    public boolean setDecimalTable( BigDecimal[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setDecimal( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public String[][] getStringTable() {
        String[][] out = new String[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getString();
            }
        }
        return out;
    }


    @Override
    public boolean setStringTable( String[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setString( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public boolean[][] getBooleanTable() {
        boolean[][] out = new boolean[table.length][table[0].length];
        for ( int i = 0; i < out.length; i++ ) {
            for ( int j = 0; j < out[0].length; j++ ) {
                out[i][j] = this.table[i][j].getBoolean();
            }
        }
        return out;
    }


    @Override
    public boolean setBooleanTable( boolean[][] value ) {
        if( validate ( value )) {
            for ( int i = 0; i < table.length; i++ ) {
                for ( int j = 0; j < table[0].length; j++ ) {
                    table[i][j].setBoolean( value[i][j] );
                }
            }
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


}
