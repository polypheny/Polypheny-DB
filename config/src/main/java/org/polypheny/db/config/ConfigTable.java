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


public class ConfigTable extends Config {

    private ConfigScalar[][] table;
    private ConfigScalar[][] defaultTable;


    public ConfigTable( final String key, final int[][] array ) {
        super( key );
        ConfigScalar[][] fill = new ConfigScalar[array.length][array[0].length];
        for ( int i = 0; i < array.length; i++ ) {
            for ( int j = 0; j < array[0].length; j++ ) {
                fill[i][j] = (ConfigScalar) new ConfigInteger( key + "." + i + "." + j, array[i][j] ).isObservable( false );
            }
        }
        this.table = fill;
        this.defaultTable = this.table;
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
        this.defaultTable = this.table;
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
        this.defaultTable = this.table;
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
        this.defaultTable = this.table;
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
        this.defaultTable = this.table;
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
        this.defaultTable = this.table;
    }


    @Override
    public Object getPlainValueObject() {
        return table;
    }


    @Override
    public Object getDefaultValue() {
        return defaultTable;
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
        return Arrays.equals( table, defaultTable );
    }


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    @Override
    public void resetToDefault() {
        table = defaultTable.clone();
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


    @Override
    public boolean parseStringAndSetValue( String value ) {
        throw new ConfigRuntimeException( "Parse and set is not implemented for this type." );
    }

}
