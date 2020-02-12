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

package ch.unibas.dmi.dbis.polyphenydb.config;


import ch.unibas.dmi.dbis.polyphenydb.config.exception.ConfigRuntimeException;
import java.math.BigDecimal;


public class ConfigTable extends Config {

    private ConfigScalar[][] table;


    public ConfigTable( final String key, final int[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigInteger( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable( final String key, final double[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigDouble( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable( final String key, final long[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigLong( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable( final String key, final BigDecimal[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigDecimal( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable( final String key, final String[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigString( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
    }


    public ConfigTable( final String key, final boolean[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigBoolean( key + "." + i + "." + j, array[i][j] ).isObservable( false );
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
    public boolean setIntTable( final int[][] value ) {
        if ( validate( value ) ) {
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
    public boolean setDoubleTable( final double[][] value ) {
        if ( validate( value ) ) {
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
        if ( validate( value ) ) {
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
    public boolean setDecimalTable( final BigDecimal[][] value ) {
        if ( validate( value ) ) {
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
    public boolean setStringTable( final String[][] value ) {
        if ( validate( value ) ) {
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
    public boolean setBooleanTable( final boolean[][] value ) {
        if ( validate( value ) ) {
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


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        throw new ConfigRuntimeException( "Reading tables of values from config files is not supported yet." );
    }

}
