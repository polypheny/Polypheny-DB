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

package org.polypheny.db.adaptimizer.environment;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.polypheny.db.adaptimizer.except.TestDataGenerationException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.type.PolyType;

/**
 * Supplies random filler-data for tables. Adheres to key constraints.
 */
public class DataRecordSupplier implements Supplier<List<Object>> {
    private static final SimpleDateFormat SDF_DATE = new SimpleDateFormat("dd/MM/yyyy");
    private static final SimpleDateFormat SDF_TIME = new SimpleDateFormat("hh:mm:ss");
    private static final SimpleDateFormat SDF_TIME_Z = new SimpleDateFormat("hh:mm:ss z");
    private static final SimpleDateFormat SDF_TIME_STAMP = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
    private static final SimpleDateFormat SDF_TIME_STAMP_Z = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss z");

    private final List<CatalogColumn> columns;
    private final List<Long> columnIds;
    private final HashMap<Long, DataColumnOption> options;
    private final Random random;
    private final int varLength;

    private int counter;

    public DataRecordSupplier( Long tableId, HashMap<Long, DataColumnOption> options, int varLength ) {
        Catalog catalog = Catalog.getInstance();
        this.columns = catalog.getColumns( tableId );
        this.options = options;
        this.varLength = varLength;
        this.random = new Random();
        this.counter = 0;

        this.columnIds = this.columns.stream().map( column -> column.id ).collect( Collectors.toList());
    }


    /**
     * Returns a random value for a given CatalogColumn.
     * @param column        Column to generate a value for.
     * @return              Random Object of the derived PolyType.
     */
    private Object randomValueOf( CatalogColumn column ) {

        if ( options.containsKey( column.id ) ) {
            DataColumnOption testDataColumnOption =  options.get( column.id );
            if ( testDataColumnOption.isPrimaryKeyColumn() ) {
                return getNextUniqueValue( column.type );
            } else if ( testDataColumnOption.isForeignKeyColumn() ) {
                return testDataColumnOption.getNextReferencedValue( random );
            }
        }

        return getRandomDataForType( column.type );
    }

    private Object randomUniqueDate() {
        long max = 0L; // Sets Bounds
        long min = 100000000000L;

        return SDF_DATE.format( new Date( ( counter % ( max - min ) ) + min ) );
    }

    private Object randomDate() {
        long max = 0L; // Sets Bounds
        long min = 100000000000L;

        return SDF_DATE.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
    }


    /**
     * Generates a random value for the given type, where the value is unique in the context of this class instance.
     */
    private Object getNextUniqueValue( PolyType polyType ) {
        switch ( polyType ) {
            case TINYINT:
                return (byte) counter;
            case SMALLINT:
                return (short) counter;
            case INTEGER:
                return (int) counter;
            case BIGINT:
                return (long) counter;
            case DECIMAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "DECIMAL" ) );
            case FLOAT:
                return (float) 1 / (1 + counter);
            case REAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "REAL" ) );
            case DOUBLE:
                return (double) counter;
            case DATE:
                return randomUniqueDate();
            case TIME:
                return randomTime( false );
            case TIME_WITH_LOCAL_TIME_ZONE:
                return randomTime( true );
            case TIMESTAMP:
                return randomTimeStamp( false );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return randomTimeStamp( true );
            case CHAR:
                return (char)(random.nextInt(26) + 'a');
            case VARCHAR:
                return RandomStringUtils.randomAlphabetic( varLength - ( String.valueOf( counter ) ).length() ) + counter;
            default:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( polyType.getName() ) );
        }
    }


    /**
     * Generates a random value for the given type.
     */
    private Object getRandomDataForType( PolyType polyType ) {
        switch ( polyType ) {
            case BOOLEAN:
                return this.random.nextInt( 2 ) == 0;
            case TINYINT:
                return this.random.nextInt( 256 ) - 128;
            case SMALLINT:
                return this.random.nextInt( 65536 ) - 32768;
            case INTEGER:
                return this.random.nextInt();
            case BIGINT:
                return this.random.nextLong();
            case DECIMAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "DECIMAL" ) );
            case FLOAT:
                return this.random.nextFloat();
            case REAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "REAL" ) );
            case DOUBLE:
                return this.random.nextDouble();
            case DATE:
                return this.randomDate();
            case TIME:
                return this.randomTime( false );
            case TIME_WITH_LOCAL_TIME_ZONE:
                return this.randomTime( true );
            case TIMESTAMP:
                return this.randomTimeStamp( false );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this.randomTimeStamp( true );
            case INTERVAL_YEAR:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "INTERVAL" ) );
            case CHAR:
                return (char)( this.random.nextInt( 26 ) + 'a' );
            case VARCHAR:
                return RandomStringUtils.randomAlphanumeric( varLength );
            case BINARY:
                return this.random.nextInt( 2 );
            case VARBINARY:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "VARBINARY" ) );
            case NULL:
                return null;
            case ANY:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "ANY" ) );
            case SYMBOL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "SYMBOL" ) );
            case MULTISET:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "MULTISET" ) );
            case ARRAY:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "ARRAY" ) );
            case MAP:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "MAP" ) );
            case DISTINCT:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "DISTINCT" ) );
            case STRUCTURED:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "STRUCTURED" ) );
            case ROW:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "ROW" ) );
            case OTHER:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "OTHER" ) );
            case CURSOR:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "CURSOR" ) );
            case COLUMN_LIST:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "COLUMN_LIST" ) );
            case DYNAMIC_STAR:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "DYNAMIC_STAR" ) );
            case GEOMETRY:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "GEOMETRY" ) );
            case FILE:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "FILE" ) );
            case IMAGE:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "IMAGE" ) );
            case VIDEO:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "VIDEO" ) );
            case SOUND:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "SOUND" ) );
            case JSON:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "JSON" ) );
            default:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( polyType.getName() ) );
        }
    }

    private Object randomTime( boolean localTimeZone ) {
        long max = 0L; // Sets Bounds
        long min = 100000000000L;

        if ( localTimeZone ) {
            return SDF_TIME_Z.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        } else {
            return SDF_TIME.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        }
    }

    private Object randomTimeStamp( boolean localTimeZone ) {
        long max = 0L; // Sets Bounds
        long min = 100000000000L;

        if ( localTimeZone ) {
            return SDF_TIME_STAMP_Z.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        } else {
            return SDF_TIME_STAMP.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        }
    }


    /**
     * Returns random records of data.
     */
    @Override
    public List<Object> get() {
        this.counter++;
        return this.columns.stream().map( this::randomValueOf ).collect( Collectors.toList());
    }


    /**
     * Returns column-ids corresponding to the randomly generated records.
     */
    public List<Long> getColumnIds() {
        return this.columnIds;
    }

}
