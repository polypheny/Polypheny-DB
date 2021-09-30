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

package org.polypheny.db.adapter.ethereum;

import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.time.FastDateFormat;


/**
 * Enumerator that reads from a Blockchain.
 *
 * @param <E> Row type
 */
class EthereumEnumerator<E> implements Enumerator<E> {

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;


    static {
        final TimeZone gmt = TimeZone.getTimeZone( "GMT" );
        TIME_FORMAT_DATE = FastDateFormat.getInstance( "yyyy-MM-dd", gmt );
        TIME_FORMAT_TIME = FastDateFormat.getInstance( "HH:mm:ss", gmt );
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance( "yyyy-MM-dd HH:mm:ss", gmt );
    }


    private final String clientUrl;
    private final BlockReader reader;
    private final String[] filterValues;
    private final AtomicBoolean cancelFlag;
    private final RowConverter<E> rowConverter;
    private final int blocks;
    private E current;


    EthereumEnumerator( String clientUrl, int blocks, AtomicBoolean cancelFlag, boolean stream, String[] filterValues, EthereumMapper mapper, Predicate<BigInteger> blockNumberPredicate, RowConverter<E> rowConverter ) {
        this.clientUrl = clientUrl;
        this.cancelFlag = cancelFlag;
        this.rowConverter = rowConverter;
        this.filterValues = filterValues;
        this.reader = mapper.makeReader( clientUrl, blocks, blockNumberPredicate );
        this.blocks = blocks;
    }


    static RowConverter<?> converter( List<EthereumFieldType> fieldTypes, int[] fields ) {
        if ( fields.length == 1 ) {
            final int field = fields[0];
            return new SingleColumnRowConverter( fieldTypes.get( field ), field );
        } else {
            return new ArrayRowConverter( fieldTypes, fields );
        }
    }


    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    static int[] identityList( int n ) {
        int[] integers = new int[n];
        for ( int i = 0; i < n; i++ ) {
            integers[i] = i;
        }
        return integers;
    }


    @Override
    public E current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            outer:
            for ( ; ; ) {
                if ( cancelFlag.get() ) {
                    return false;
                }
                final String[] strings = reader.readNext();
                if ( strings == null ) {
                    return false;
                }
                if ( filterValues != null ) {
                    for ( int i = 0; i < strings.length; i++ ) {
                        String filterValue = filterValues[i];
                        if ( filterValue != null ) {
                            if ( !filterValue.equals( strings[i] ) ) {
                                continue outer;
                            }
                        }
                    }
                }
                current = rowConverter.convertRow( strings );
                return true;
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        try {
            reader.close();
        } catch ( IOException e ) {
            throw new RuntimeException( "Error closing Blockchain reader", e );
        }
    }


    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {

        abstract E convertRow( String[] rows );


        protected Object convert( EthereumFieldType fieldType, String string ) {
            if ( fieldType == null ) {
                return string;
            }
            switch ( fieldType ) {
                case BOOLEAN:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Boolean.parseBoolean( string );
                case BYTE:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Byte.parseByte( string );
                case SHORT:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Short.parseShort( string );
                case INT:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Integer.parseInt( string );
                case LONG:
                    if ( string.length() == 0 ) {
                        return null;
                    }

                    return new BigInteger( string );
                case FLOAT:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Float.parseFloat( string );
                case DOUBLE:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    return Double.parseDouble( string );
                case DATE:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_DATE.parse( string );
                        return (int) (date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
                    } catch ( ParseException e ) {
                        return null;
                    }
                case TIME:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIME.parse( string );
                        return (int) date.getTime();
                    } catch ( ParseException e ) {
                        return null;
                    }
                case TIMESTAMP:
                    if ( string.length() == 0 ) {
                        return null;
                    }
                    try {
                        Date date = new Date( Long.parseLong( string ) * 1000 );
                        return date.getTime();
                    } catch ( Exception e ) {
                        return null;
                    }
                case STRING:
                default:
                    return string;
            }
        }

    }


    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter<Object[]> {

        private final EthereumFieldType[] fieldTypes;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;


        ArrayRowConverter( List<EthereumFieldType> fieldTypes, int[] fields ) {
            this.fieldTypes = fieldTypes.toArray( new EthereumFieldType[0] );
            this.fields = fields;
            this.stream = false;
        }


        ArrayRowConverter( List<EthereumFieldType> fieldTypes, int[] fields, boolean stream ) {
            this.fieldTypes = fieldTypes.toArray( new EthereumFieldType[0] );
            this.fields = fields;
            this.stream = stream;
        }


        @Override
        public Object[] convertRow( String[] strings ) {
            if ( stream ) {
                return convertStreamRow( strings );
            } else {
                return convertNormalRow( strings );
            }
        }


        public Object[] convertNormalRow( String[] strings ) {
            final Object[] objects = new Object[fields.length];
            for ( int i = 0; i < fields.length; i++ ) {
                int field = fields[i];
                objects[i] = convert( fieldTypes[i], strings[field] );
            }
            return objects;
        }


        public Object[] convertStreamRow( String[] strings ) {
            final Object[] objects = new Object[fields.length];
            objects[0] = System.currentTimeMillis();
            for ( int i = 0; i < fields.length; i++ ) {
                int field = fields[i];
                objects[i] = convert( fieldTypes[i], strings[field] );
            }
            return objects;
        }

    }


    /**
     * Single column row converter.
     */
    private static class SingleColumnRowConverter extends RowConverter {

        private final EthereumFieldType fieldType;
        private final int fieldIndex;


        private SingleColumnRowConverter( EthereumFieldType fieldType, int fieldIndex ) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }


        @Override
        public Object convertRow( String[] strings ) {
            return convert( fieldType, strings[fieldIndex] );
        }

    }

}
