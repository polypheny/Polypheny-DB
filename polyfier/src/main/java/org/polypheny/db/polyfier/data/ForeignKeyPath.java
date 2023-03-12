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

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.commons.lang3.tuple.Triple;


public class ForeignKeyPath {
    private final Map<Long, Long> seed;
    private final Map<Long, ColumnOption> options;
    private final Map<Long, Integer> cSize;
    private final int length;

    private final Integer[] counters;
    private final Random[] randoms;
    private final Long[] columns;

    public ForeignKeyPath( final Map<Long, Long> seed, final Map<Long, ColumnOption> options, final Map<Long, Integer> cSize, final ColumnOption option ) {
        final ArrayList<Integer> counters = new ArrayList<>();
        final ArrayList<Random> randoms = new ArrayList<>();
        final ArrayList<Long> columns = new ArrayList<>();

        this.seed = seed;
        this.options = options;
        this.cSize = cSize;
        this.add( seed, options, option.getCatalogColumn(), counters, randoms, columns );
        this.init( options.get( option.getReferencedColumn() ), counters, randoms, columns );
        this.counters = counters.toArray( Integer[]::new );
        this.randoms = randoms.toArray( Random[]::new );
        this.columns = columns.toArray( Long[]::new );
        this.length = this.columns.length;
    }

    private void init( final ColumnOption option, final ArrayList<Integer> counters, final ArrayList<Random> randoms, final ArrayList<Long> columns  ) {
        if ( Objects.equals( option.getReferencedColumn(), columns.get( 0 ) ) ) {
            option.setReferencedColumn( null ); // cut cycle
        }
        this.add( seed, options, option.getCatalogColumn(), counters, randoms, columns );
        if ( option.getReferencedColumn() != null ) {
            init( options.get( option.getReferencedColumn() ), counters, randoms, columns );
        }
    }

    private void add( Map<Long, Long> seed, Map<Long, ColumnOption> options, Long column,
            ArrayList<Integer> counters, ArrayList<Random> randoms, ArrayList<Long> columns ) {
        counters.add( 0 );
        randoms.add( new Random( seed.get( column ) ) );
        columns.add( options.get( column ).getCatalogColumn() );
    }

    private boolean nextColumnOutOfBounds( int i ) {
        return counters[ i ] > cSize.get( columns[ i + 1 ] );
    }

    private void update() {
        for ( int i = 0; i < length - 1; i++ ) {
            if ( nextColumnOutOfBounds( i ) ) {
                reset( 0, length );
                break;
            }
        }
    }

    public void reset() {
        reset( 0, length );
    }

    private void increment() {
        for (  int i = 0; i < length; i++ ) {
            counters[ i ]++;
        }
    }

    private void reset(int i, int j ) {
        for ( int k = i; k < j; k++ ) {
            counters[ k ] = 1;
            randoms[ k ] = new Random( seed.get( columns[ k ] ) );
        }
    }

    public Object traverse( Object obj ) {
        ColumnOption option;
        for ( int i = length - 2; i >= 0; i-- ) {
            option = options.get( columns[ i ] );
            if ( option.isForeignKeyRoutine() ) {
                obj = option.getFkRoutine().apply( obj, getTriple( i ) );
            }
        }
        return obj;
    }

    private Triple<Long, Random, Integer> getTriple( int i ) {
        return Triple.of( columns[ i ], randoms[ i ], counters[ i ] );
    }

    public Triple<Long, Random, Integer> getGoalColumn() {
        increment();
        update();
        return getTriple( length - 1 );
    }

    public Long getStartColumn() {
        return columns[ 0 ];
    }

}
