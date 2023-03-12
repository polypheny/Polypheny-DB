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

package org.polypheny.db.polyfier.data;

import com.google.common.collect.ImmutableMap;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.polyfier.core.PolyfierException;

/**
 * Supplies random filler-data for tables.
 */
@Slf4j
public class DataRecordGenerator {

    /**
     * Contains initial seeds for random objects of columns.
     */
    @Getter(AccessLevel.MODULE)
    private final ImmutableMap<Long, Long> seed;

    /**
     * Contains options for columns {@link ColumnOption}
     */
    @Getter(AccessLevel.MODULE)
    private final ImmutableMap<Long, ColumnOption> options;

    /**
     * Contains the sizes for columns / tables the columns belong to.
     */
    @Getter(AccessLevel.MODULE)
    private final ImmutableMap<Long, Integer> cSize;

    /**
     * Contains the sizes for columns / tables the columns belong to.
     */
    @Getter(AccessLevel.MODULE)
    private final ImmutableMap<Long, Integer> tSize;

    /**
     * Mapping that handles foreign key reference generation.
     */
    @Getter(AccessLevel.PRIVATE)
    private final ForeignKeyPaths foreignKeyPaths;

    /**
     * Contains the random objects associated with the columns in their current config.
     */
    @Getter(AccessLevel.PRIVATE)
    private Map<Long, Random> rnd;

    /**
     * Contains counters incremented if an object is generated for the associated column.
     */
    @Getter(AccessLevel.PRIVATE)
    private Map<Long, Integer> count;

    /**
     * Current catalog column in consideration during generation.
     */
    @Getter(AccessLevel.PRIVATE)
    private Long currentColumn;

    /**
     * Current catalog column in consideration during generation.
     */
    @Getter(AccessLevel.PRIVATE)
    private Random currentRandom;

    /**
     * Current catalog column in consideration during generation.
     */
    @Getter(AccessLevel.PRIVATE)
    private Integer currentCounter;

    private final Catalog catalog;


    public DataRecordGenerator( Map<Long, ColumnOption> options, Map<Long, Integer> tSize, Map<Long, Integer> cSize, Map<Long, Long> seed ) {
        this.cSize = ImmutableMap.copyOf( cSize );
        this.tSize = ImmutableMap.copyOf( tSize );
        this.options = ImmutableMap.copyOf( options );
        this.seed = ImmutableMap.copyOf( seed );
        this.foreignKeyPaths = new ForeignKeyPaths( seed, options, cSize, options.values() );
        resetRnd();
        this.catalog = Catalog.getInstance();
    }

    // ----------------- Interface Functions ----------------------

    /**
     * Sets the counter & the seed for all columns to initial configurations.
     */
    public void resetRnd() {
        rnd = seed.entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> new Random( e.getValue() ) ) );
        count = new ArrayList<>( options.keySet() ).stream().collect( Collectors.toMap( c -> c, c -> 0 ) );
        foreignKeyPaths.reset();
    }

    public List<Object> generateFor( List<Long> columns ) {
        return columns.stream().map( this::randomValueOf ).collect( Collectors.toList());
    }

    public List<Object> generateFor( Long table ) {
        return generateFor( columnsOf( table ) );
    }
    public List<Long> columnsOf( Long table ) {
        return Catalog.getInstance().getTable( table ).fieldIds;
    }

    public List<CatalogColumn> catalogColumnsOf( List<Long> columns ) {
        return columns.stream().map( catalog::getColumn ).collect( Collectors.toList() );
    }

    public List<CatalogColumn> getColumns( Long table ) {
        return catalogColumnsOf( columnsOf( table ) );
    }

    // ----------------- Record Generation ---------------------------

    /**
     * Returns a random value for a given CatalogColumn.
     * @param column        {@link CatalogColumn} id to generate a value for.
     * @return              Random Object of the derived PolyType with constraints provided by options.
     */
    private Object randomValueOf( Long column ) {
        ForeignKeyPath foreignKeyPath = foreignKeyPaths.get( column );

        if ( foreignKeyPath != null ) {
            Triple<Long, Random, Integer> goal = foreignKeyPath.getGoalColumn();

            currentColumn = goal.getLeft();
            currentRandom = goal.getMiddle();
            currentCounter = goal.getRight();

            return foreignKeyPath.traverse( nextValue() ); // Todo expand traversal
        } else {
            count.put( column, count.get( column ) + 1 );

            currentColumn = column;
            currentRandom = rnd.get( column );
            currentCounter = count.get( column );

            return nextValue();
        }
    }


    /**
     * Provides a next random value for an option.
     */
    private Object nextValue() {
        ColumnOption option = options.get( currentColumn );

        // Check Null Option:
        if ( isNull( option ) ) {
            return null;
        }

        // Check Uniform Values:
        Object obj = checkUniform( option );
        if ( obj != null ) {
            return obj;
        }

        switch ( option.getType() ) {
            case BOOLEAN:
                return randomBool( option );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return randomInt( option );
            case DECIMAL:
                return randomDecimal( option );
            case FLOAT:
                return randomFloat( option );
            case REAL:
                return randomReal( option );
            case DOUBLE:
                return randomDouble( option );
            case DATE:
                return randomDate( option );
            case TIME:
                return randomTime( option, false );
            case TIME_WITH_LOCAL_TIME_ZONE:
                return randomTime( option, true );
            case TIMESTAMP:
                return randomTimestamp( option, false );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return randomTimestamp( option, true );
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
                return randomInterval( option );
            case CHAR:
                return randomChar( option );
            case VARCHAR:
                return randomVarchar( option );
            case BINARY:
                return randomBinary( option );
            default:
                throw exceptType( option );

        }
    }

    // ------------------ Random Object Functions ----------------------

    private Object randomBinary( ColumnOption option ) {
        return getCurrentRandom().nextInt( 2 );
    }

    private Object randomInterval( ColumnOption option ) {
        // Todo interval cases
        throw exceptType( option );
    }

    private Object randomChar( ColumnOption option ) {
        return (char)( getCurrentCounter() + 'a' ); // Todo chars modifications
    }

    private Object randomVarchar( ColumnOption option ) {
        // Todo varchar modifications
        if ( ! option.isModified() ) {
            return RandomStringUtils.randomAlphanumeric( Options.DEFAULT_VARCHAR_LENGTH );
        } else if ( option.getLength() != null ) {
            return RandomStringUtils.randomAlphanumeric( Options.DEFAULT_VARCHAR_LENGTH );
        } else if ( option.isUnique() ) {
            return RandomStringUtils.randomAlphanumeric( Options.DEFAULT_VARCHAR_LENGTH );
        } else {
            debugLogUnusedOption( option );
            return RandomStringUtils.randomAlphanumeric( Options.DEFAULT_VARCHAR_LENGTH );
        }
    }

    private Object randomFloat( ColumnOption option ) {
        if ( ! option.isModified() ) {
            return getCurrentRandom().nextFloat();
        } else if ( option.isUnique() ) {
            return 1 / ( getCurrentCounter() + 1 );
        } else if ( option.getFloatRange() != null ) {
            return option.getFloatRange().left + getCurrentRandom().nextFloat() * ( option.getFloatRange().right - option.getFloatRange().left );
        } else {
            debugLogUnusedOption( option );
            return getCurrentRandom().nextFloat();
        }
    }

    private Object randomReal( ColumnOption option ) {
        // Todo real numbers
        throw exceptType( option );
    }

    private Object randomDecimal( ColumnOption option ) {
        // Todo decimal numbers
        throw exceptType( option );
    }

    private Object randomDouble( ColumnOption option ) {
        if ( ! option.isModified() ) {
            return getCurrentRandom().nextDouble();
        } else if ( option.isUnique() ) {
            return 1 / ( getCurrentCounter() + 1 );
        } else if ( option.getDoubleRange() != null ) {
            return option.getDoubleRange().left + getCurrentRandom().nextDouble() * ( option.getDoubleRange().right - option.getDoubleRange().left );
        } else {
            debugLogUnusedOption( option );
            return getCurrentRandom().nextDouble();
        }
    }

    private Object randomBool( ColumnOption option ) {
        if ( ! option.isModified() ) {
            return getCurrentRandom().nextBoolean();
        } else if ( option.isUnique() ) {
            throw exceptPk( option );
        } else {
            debugLogUnusedOption( option );
            return getCurrentRandom().nextBoolean();
        }
    }

    private Object randomInt( ColumnOption option) {
        if ( ! option.isModified() ) {
            return randomIntDef( option );
        } else if ( option.isUnique() ) {
            return getCurrentCounter();
        } else if ( option.getIntRange() != null ) {
            return getCurrentRandom().nextInt( option.getIntRange().right - option.getIntRange().left ) + option.getIntRange().left;
        } else {
            debugLogUnusedOption( option );
            return randomIntDef( option );
        }
    }

    private Object randomIntDef( ColumnOption option) {
        switch ( option.getType() ) {
            case TINYINT:
                return getCurrentRandom().nextInt( 256 ) - 128;
            case SMALLINT:
                return getCurrentRandom().nextInt( 65536 ) - 32768;
            case INTEGER:
                return getCurrentRandom().nextInt();
            case BIGINT:
                return getCurrentRandom().nextLong();
            default:
                throw exceptType( option );
        }
    }

    private Object randomDate( ColumnOption option) {
        if ( ! option.isModified() ) {
            return randomDate();
        } else if ( option.isUnique() ) {
            throw exceptPk( option );
        } else if ( option.getLongRange() != null ) {
            return randomDate( option.getLongRange().left, option.getLongRange().right );
        } else {
            debugLogUnusedOption( option );
            return randomDate();
        }
    }

    private Object randomTime( ColumnOption option, boolean lz) {
        if ( ! option.isModified() ) {
            return randomTime( lz );
        } else if ( option.isUnique() ) {
            throw exceptPk( option );
        } else if ( option.getLongRange() != null ) {
            return randomTime( option.getLongRange().left, option.getLongRange().right, lz );
        } else {
            debugLogUnusedOption( option );
            return randomTime( lz );
        }
    }

    private Object randomTimestamp( ColumnOption option, boolean lz) {
        if ( ! option.isModified() ) {
            return randomTimeStamp( lz );
        } else if ( option.isUnique() ) {
            throw exceptPk( option );
        } else if ( option.getLongRange() != null ) {
            return randomTimeStamp( option.getLongRange().left, option.getLongRange().right, lz );
        } else {
            debugLogUnusedOption( option );
            return randomTimeStamp( lz );
        }
    }

    // ------------------- Datetime Auxiliary Functions ------------------------

    private Object formatBounds( boolean lz, SimpleDateFormat sdfLz, SimpleDateFormat sdf, Long max, Long min ) {
        Date date = new Date( ( getCurrentRandom().nextLong() % ( max - min ) ) + min );
        return ( lz ) ? sdfLz.format( date ) : sdf.format( date );
    }

    private Object randomDate() {
        return randomDate( 0L, 100000000000L  );
    }

    private Object randomDate( long start, long end) {
        return formatBounds( false, null, Options.SDF_DATE, start, end  );
    }

    private Object randomTime( boolean localTimeZone ) {
        return randomTime( 0L, 100000000000L, localTimeZone  );
    }

    private Object randomTime( long start, long end, boolean lz) {
        return formatBounds( lz, Options.SDF_TIME_Z, Options.SDF_TIME, start, end  );
    }

    private Object randomTimeStamp( boolean localTimeZone ) {
        return randomTimeStamp( 0L, 100000000000L, localTimeZone  );
    }

    private Object randomTimeStamp( long start, long end, boolean lz) {
        return formatBounds( lz, Options.SDF_TIME_STAMP_Z, Options.SDF_TIME_STAMP, start, end  );
    }

    // ----------------- Other Auxiliary ----------------------

    private boolean isNull( ColumnOption option ) {
        return option.isModified() && option.getNullProbability() != null && getCurrentRandom().nextFloat() < option.getNullProbability();
    }

    private Object checkUniform( ColumnOption option ) {
        if ( option.isModified() && option.getUniformValues() != null ) {
            if ( option.isUnique() ) {
                throw exceptPk( option );
            }
            return option.getUniformValues().get( getCurrentRandom().nextInt( option.getUniformValues().size() ) );
        }
        return null;
    }

    private PolyfierException exceptType(ColumnOption option ) {
        return new PolyfierException( String.format( "Type not implemented: %s", option.getType().getName()), new IllegalArgumentException("") );
    }


    private PolyfierException exceptPk( ColumnOption option ) {
        return new PolyfierException( String.format( "Primary key not allowed for %s", option.getType().getName() ), new IllegalArgumentException("") );
    }

    private void debugLogUnusedOption( ColumnOption option ) {
        log.debug( "Did not use option for column {}, possible error in config", option.getType().getName() );
    }


}
