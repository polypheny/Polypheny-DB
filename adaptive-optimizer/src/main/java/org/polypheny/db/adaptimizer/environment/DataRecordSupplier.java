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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.polypheny.db.adaptimizer.except.TestDataGenerationException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Supplies random filler-data for tables.
 */
@Slf4j
public class DataRecordSupplier implements Supplier<List<Object>> {
    private final List<CatalogColumn> columns;
    private final List<Long> columnIds;
    private final Map<Long, DataColumnOption> options;
    private final Random random;

    private int counter;

    public DataRecordSupplier( Catalog catalog, CatalogTable catalogTable, Map<Long, DataColumnOption> options ) {
        this.columns = catalog.getColumns( catalogTable.id );
        this.columnIds = this.columns.stream().map( column -> column.id ).collect( Collectors.toList());
        this.options = options;
        this.random = new Random();
        this.counter = 0;
    }

    /**
     * Returns a random value for a given CatalogColumn.
     * @param column        {@link CatalogColumn} to generate a value for.
     * @return              Random Object of the derived PolyType with constraints provided by options.
     */
    private Object randomValueOf( CatalogColumn column ) {
        if ( options.containsKey( column.id ) ) {
            return nextValue( true, options.get( column.id ), column.type );
        }
        return nextValue( false, null, column.type );
    }


    private Object randomDate() {
        return randomDate( 0L, 100000000000L );
    }

    private Object randomDate( Long start, Long end ) {
        long max = start; // Sets Bounds
        long min = end;

        return DataDefinitions.SDF_DATE.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
    }

    private Object randomTime( boolean localTimeZone ) {
        return randomTime( 0L, 100000000000L, localTimeZone );
    }

    private Object randomTime( Long start, Long end, boolean localTimeZone ) {
        long max = start; // Sets Bounds
        long min = end;

        if ( localTimeZone ) {
            return DataDefinitions.SDF_TIME_Z.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        } else {
            return DataDefinitions.SDF_TIME.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        }
    }

    private Object randomTimeStamp( boolean localTimeZone ) {
        return randomTimeStamp( 0L, 100000000000L, localTimeZone );
    }

    private Object randomTimeStamp( Long start, Long end, boolean localTimeZone ) {
        long max = start; // Sets Bounds
        long min = end;

        if ( localTimeZone ) {
            return DataDefinitions.SDF_TIME_STAMP_Z.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        } else {
            return DataDefinitions.SDF_TIME_STAMP.format( new Date( ( random.nextLong() % ( max - min ) ) + min ) );
        }
    }


    private Object nextValue(boolean hasOptions, DataColumnOption dataColumnOption, PolyType polyType) {

        // Check Null Option:
        if ( hasOptions && dataColumnOption.getNullProbability() != null && random.nextFloat() < dataColumnOption.getNullProbability() ) {
            return null;
        }
        if ( hasOptions && dataColumnOption.getProvidedData() != null ) {
            return dataColumnOption.getNextReferencedValue( random );
        }
        // Check Uniform Values:
        if ( hasOptions && dataColumnOption.getUniformValues() != null ) {
            if ( dataColumnOption.getUnique() ) {
                return dataColumnOption.getUniformValues().get( counter );
            }
            return dataColumnOption.getUniformValues().get( random.nextInt( dataColumnOption.getUniformValues().size() ) );
        }

        switch ( polyType ) {
            case BOOLEAN:
                if ( hasOptions && dataColumnOption.getUnique() ) {
                    throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                }
                return this.random.nextInt( 2 ) == 0;
            case TINYINT:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return counter;
                    } else if ( dataColumnOption.getIntRange() != null ) {
                        Pair<Integer, Integer> range = dataColumnOption.getIntRange();
                        return this.random.nextInt( range.right - range.left ) + range.left;
                    }
                }
                return this.random.nextInt( 256 ) - 128;
            case SMALLINT:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return counter;
                    } else if ( dataColumnOption.getIntRange() != null  ) {
                        Pair<Integer, Integer> range = dataColumnOption.getIntRange();
                        return this.random.nextInt( range.right - range.left ) + range.left;
                    }
                }
                return this.random.nextInt( 65536 ) - 32768;
            case INTEGER:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return counter;
                    } else if ( dataColumnOption.getIntRange() != null  ) {
                        Pair<Integer, Integer> range = dataColumnOption.getIntRange();
                        return this.random.nextInt( range.right - range.left ) + range.left;
                    }
                }
                return this.random.nextInt();
            case BIGINT:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return counter;
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return range.left + random.nextLong() * ( range.right - range.left );
                    }
                }
                return this.random.nextLong();
            case DECIMAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "DECIMAL" ) );
            case FLOAT:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return 1 / ( counter + 1 );
                    } else if ( dataColumnOption.getFloatRange() != null  ) {
                        Pair<Float, Float> range = dataColumnOption.getFloatRange();
                        return range.left + random.nextFloat() * ( range.right - range.left );
                    }
                }
                return this.random.nextFloat();
            case REAL:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( "REAL" ) );
            case DOUBLE:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        return 1 / ( counter + 1 );
                    } else if ( dataColumnOption.getDoubleRange() != null  ) {
                        Pair<Double, Double> range = dataColumnOption.getDoubleRange();
                        return range.left + random.nextDouble() * ( range.right - range.left );
                    }
                }
                return this.random.nextDouble();
            case DATE:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return this.randomDate( range.left, range.right );
                    }
                }
                return this.randomDate();
            case TIME:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return this.randomTime( range.left, range.right, false );
                    }
                }
                return this.randomTime( false );
            case TIME_WITH_LOCAL_TIME_ZONE:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return this.randomTime( range.left, range.right, true );
                    }
                }
                return this.randomTime( true );
            case TIMESTAMP:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return this.randomTimeStamp( range.left, range.right, false );
                    }
                }
                return this.randomTimeStamp( false );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if ( hasOptions ) {
                    if ( dataColumnOption.getUnique() ) {
                        throw new TestDataGenerationException( String.format( "Primary key not allowed for %s", polyType.getName() ), new IllegalArgumentException() );
                    } else if ( dataColumnOption.getLongRange() != null ) {
                        Pair<Long, Long> range = dataColumnOption.getLongRange();
                        return this.randomTimeStamp( range.left, range.right, true );
                    }
                }
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
                if ( hasOptions && dataColumnOption.getUnique() ) {
                    return (char)( counter + 'a' );
                }
                return (char)( this.random.nextInt( 26 ) + 'a' );
            case VARCHAR:
                if ( dataColumnOption.getLength() != null) {
                    return RandomStringUtils.randomAlphanumeric( dataColumnOption.getLength() );
                };
                return RandomStringUtils.randomAlphanumeric( DataDefinitions.DEFAULT_VARCHAR_LENGTH );
            case BINARY:
                return this.random.nextInt( 2 );
            default:
                throw new TestDataGenerationException( "Type not implemented", new IllegalArgumentException( polyType.getName() ) );
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
