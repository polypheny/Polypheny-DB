/*
 * Copyright 2019-2024 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.csv;


import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.time.FastDateFormat;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Source;


/**
 * Enumerator that reads from a CSV file.
 */
class CsvEnumerator implements Enumerator<PolyValue[]> {

    private final CSVReader reader;
    private final String[] filterValues;
    private final AtomicBoolean cancelFlag;
    private final RowConverter<PolyValue> rowConverter;
    private PolyValue[] current;

    private static final FastDateFormat TIME_FORMAT_DATE;
    private static final FastDateFormat TIME_FORMAT_TIME;
    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    /**
     * Name of the column that is implicitly created in a CSV stream table to hold the data arrival time.
     */
    private static final String ROWTIME_COLUMN_NAME = "ROWTIME";


    static {
        final TimeZone gmt = TimeZone.getTimeZone( "GMT" );
        TIME_FORMAT_DATE = FastDateFormat.getInstance( "yyyy-MM-dd", gmt );
        TIME_FORMAT_TIME = FastDateFormat.getInstance( "HH:mm:ss", gmt );
        TIME_FORMAT_TIMESTAMP = FastDateFormat.getInstance( "yyyy-MM-dd HH:mm:ss", gmt );
    }


    CsvEnumerator( Source source, AtomicBoolean cancelFlag, List<CsvFieldType> fieldTypes ) {
        this( source, cancelFlag, fieldTypes, identityList( fieldTypes.size() ) );
    }


    CsvEnumerator( Source source, AtomicBoolean cancelFlag, List<CsvFieldType> fieldTypes, int[] fields ) {
        //noinspection unchecked
        this( source, cancelFlag, false, null, (RowConverter<PolyValue>) converter( fieldTypes, fields ) );
    }


    CsvEnumerator( Source source, AtomicBoolean cancelFlag, boolean stream, String[] filterValues, RowConverter<PolyValue> rowConverter ) {
        this.cancelFlag = cancelFlag;
        this.rowConverter = rowConverter;
        this.filterValues = filterValues;
        try {
            if ( stream ) {
                this.reader = new CsvStreamReader( source );
            } else {
                this.reader = openCsv( source );
            }
            this.reader.readNext(); // skip header row
        } catch ( IOException | CsvValidationException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private static RowConverter<?> converter( List<CsvFieldType> fieldTypes, int[] fields ) {
        if ( fields.length == 1 ) {
            final int field = fields[0];
            return new SingleColumnRowConverter( fieldTypes.get( field ), field );
        } else {
            return new ArrayRowConverter( fieldTypes, fields );
        }
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of a CSV file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, List<CsvFieldType> fieldTypes ) {
        return deduceRowType( typeFactory, source, fieldTypes, false );
    }


    /**
     * Deduces the names and types of a table's columns by reading the first line of a CSV file.
     */
    static AlgDataType deduceRowType( JavaTypeFactory typeFactory, Source source, List<CsvFieldType> fieldTypes, Boolean stream ) {
        final List<AlgDataType> types = new ArrayList<>();
        final List<String> names = new ArrayList<>();
        final List<Long> ids = new ArrayList<>();
        if ( stream ) {
            names.add( ROWTIME_COLUMN_NAME );
            types.add( typeFactory.createPolyType( PolyType.TIMESTAMP ) );
        }
        try ( CSVReader reader = openCsv( source ) ) {
            String[] strings = reader.readNext();
            if ( strings == null ) {
                strings = new String[]{ "EmptyFileHasNoColumns:boolean" };
            }
            for ( String string : strings ) {
                final String name;
                final CsvFieldType fieldType;
                final int colon = string.indexOf( ':' );
                if ( colon >= 0 ) {
                    name = string.substring( 0, colon );
                    String typeString = string.substring( colon + 1 );
                    fieldType = CsvFieldType.of( typeString );
                    if ( fieldType == null ) {
                        System.out.println( "WARNING: Found unknown type: " + typeString + " in file: " + source.path() + " for column: " + name + ". Will assume the type of column is string" );
                    }
                } else {
                    name = string;
                    fieldType = null;
                }
                final AlgDataType type;
                if ( fieldType == null ) {
                    type = typeFactory.createPolyType( PolyType.VARCHAR );
                } else {
                    type = fieldType.toType( typeFactory );
                }
                names.add( name );
                types.add( type );
                ids.add( null );
                if ( fieldTypes != null ) {
                    fieldTypes.add( fieldType );
                }
            }
        } catch ( IOException | CsvValidationException e ) {
            // ignore
        }
        if ( names.isEmpty() ) {
            names.add( "line" );
            types.add( typeFactory.createPolyType( PolyType.VARCHAR ) );
        }
        return typeFactory.createStructType( ids, types, names );
    }


    public static CSVReader openCsv( Source source ) throws IOException {
        final Reader fileReader = source.reader();
        return new CSVReader( fileReader );
    }


    @Override
    public PolyValue[] current() {
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
                    if ( reader instanceof CsvStreamReader ) {
                        try {
                            Thread.sleep( CsvStreamReader.DEFAULT_MONITOR_DELAY );
                        } catch ( InterruptedException e ) {
                            throw new GenericRuntimeException( e );
                        }
                        continue;
                    }
                    current = null;
                    reader.close();
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
        } catch ( IOException | CsvValidationException e ) {
            throw new GenericRuntimeException( e );
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
            throw new GenericRuntimeException( "Error closing CSV reader", e );
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


    /**
     * Row converter.
     *
     * @param <E> element type
     */
    abstract static class RowConverter<E> {

        abstract E[] convertRow( String[] rows );


        protected PolyValue convert( CsvFieldType fieldType, String string ) {
            if ( fieldType == null ) {
                return PolyString.of( string );
            }
            if ( fieldType == CsvFieldType.STRING ) {
                return PolyString.of( string );
            }
            if ( string.isEmpty() ) {
                return PolyNull.NULL;
            }
            switch ( fieldType ) {
                case BOOLEAN:
                    return PolyBoolean.of( Boolean.parseBoolean( string ) );
                case BYTE:
                    return PolyInteger.of( Byte.parseByte( string ) );
                case SHORT:
                    return PolyInteger.of( Short.parseShort( string ) );
                case INT:
                    return PolyInteger.of( Integer.parseInt( string ) );
                case LONG:
                    return PolyLong.of( Long.parseLong( string ) );
                case FLOAT:
                    return PolyFloat.of( Float.parseFloat( string ) );
                case DOUBLE:
                    return PolyDouble.of( Double.parseDouble( string ) );
                case DATE:
                    try {
                        return PolyDate.of( TIME_FORMAT_DATE.parse( string ) );
                    } catch ( ParseException e ) {
                        return PolyNull.NULL;
                    }
                case TIME:
                    try {
                        Date date = TIME_FORMAT_TIME.parse( string );
                        return PolyTime.of( date.getTime() );
                    } catch ( ParseException e ) {
                        return PolyNull.NULL;
                    }
                case TIMESTAMP:
                    try {
                        return PolyTimestamp.of( TIME_FORMAT_TIMESTAMP.parse( string ) );
                    } catch ( ParseException e ) {
                        return PolyNull.NULL;
                    }
                default:
                    return PolyString.of( string );
            }
        }

    }


    /**
     * Array row converter.
     */
    static class ArrayRowConverter extends RowConverter<PolyValue> {

        private final CsvFieldType[] fieldTypes;
        private final int[] fields;
        // whether the row to convert is from a stream
        private final boolean stream;


        ArrayRowConverter( List<CsvFieldType> fieldTypes, int[] fields ) {
            this.fieldTypes = fieldTypes.toArray( new CsvFieldType[0] );
            this.fields = fields;
            this.stream = false;
        }


        ArrayRowConverter( List<CsvFieldType> fieldTypes, int[] fields, boolean stream ) {
            this.fieldTypes = fieldTypes.toArray( new CsvFieldType[0] );
            this.fields = fields;
            this.stream = stream;
        }


        @Override
        public PolyValue[] convertRow( String[] strings ) {
            if ( stream ) {
                return convertStreamRow( strings );
            } else {
                return convertNormalRow( strings );
            }
        }


        public PolyValue[] convertNormalRow( String[] strings ) {
            final PolyValue[] objects = new PolyValue[fields.length];
            for ( int i = 0; i < fields.length; i++ ) {
                int field = fields[i];
                objects[i] = convert( fieldTypes[i], strings[field] );
            }
            return objects;
        }


        public PolyValue[] convertStreamRow( String[] strings ) {
            final PolyValue[] objects = new PolyValue[fields.length + 1];
            objects[0] = PolyLong.of( System.currentTimeMillis() );
            for ( int i = 0; i < fields.length; i++ ) {
                int field = fields[i];
                objects[i + 1] = convert( fieldTypes[i], strings[field] );
            }
            return objects;
        }

    }


    /**
     * Single column row converter.
     */
    private static class SingleColumnRowConverter extends RowConverter<Object> {

        private final CsvFieldType fieldType;
        private final int fieldIndex;


        private SingleColumnRowConverter( CsvFieldType fieldType, int fieldIndex ) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }


        @Override
        public Object[] convertRow( String[] strings ) {
            return new PolyValue[]{ convert( fieldType, strings[fieldIndex] ) };
        }

    }

}
