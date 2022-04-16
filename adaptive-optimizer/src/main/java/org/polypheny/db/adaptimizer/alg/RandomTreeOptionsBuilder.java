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

package org.polypheny.db.adaptimizer.alg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.adaptimizer.except.AdaptiveOptTreeGenException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.type.PolyType;

/**
 * Builder for a {@link TreeGenRandom} instance. Acts as a builder for all options in random tree generation.
 */
public class RandomTreeOptionsBuilder extends AbstractRandomTreeOptionsBuilder {

    private final Map<String, Float> operatorProbabilityDistribution;
    HashMap<String, HashMap<String, PolyType>> tables;

    public RandomTreeOptionsBuilder() {
        this.operatorProbabilityDistribution = new HashMap<>();
    }

    public void setAdapter( int adapterId ) {

        Catalog catalog = Catalog.getInstance();

        List<CatalogColumnPlacement> columnPlacements = catalog.getColumnPlacementsOnAdapter( adapterId );

        columnPlacements.stream().map( placement -> catalog.getColumn( placement.columnId ) ).forEach( column -> {
            String tableName = column.getTableName();

            if ( ! this.tables.containsKey( tableName )) {
                this.tables.put( tableName, new HashMap<>() );
            }

            this.tables.get( tableName ).put( column.name, column.type );

        } );

    }

    /**
     * Adds an option, which should be a type of operator like "Join" or "Sort"... Together with a probability
     * of this operator being given by the built {@link TreeGenRandom} instance.
     */
    public void addOption(String type, float probability) {
        if ( Objects.equals( type, "TableScan" ) ) {
            throw new IllegalArgumentException("table scans are excluded from random tree generation");
        }
        this.operatorProbabilityDistribution.put( type, probability );
    }

    public void addOptions(String[] types, float[] probabilities) {
        if ( types.length != probabilities.length ) {
            throw new IllegalArgumentException("arrays of unequal length");
        }
        for ( int i = 0; i < types.length; i++ ) {
            addOption( types[i], probabilities[i] );
        }
    }


    /**
     * Builds a new TreeGenRandom object that can return random operators.
     */
    public RandomTreeOptions build() {
        if ( this.tables == null ) {
            throw new AdaptiveOptTreeGenException( "no adapter specified" , new NullPointerException() );
        }

        // mapping the floats to integers, and integers to an array containing indexes for operators
        ArrayList<Integer> values = ( ArrayList<Integer> ) this.operatorProbabilityDistribution.values().stream().map( f -> ( int )( f * 100) ).sorted().collect( Collectors.toList() );
        int sum = values.stream().mapToInt( i -> i ).sum();

        int[] distribution = new int[sum];

        int i = 0;
        int j = 0;
        int k = 0;
        while ( i < sum ) {
            distribution[ i ] = k;
            if ( j < values.get( k ) ) {
                j++;
            } else {
                j = 0;
                k++;
            }
            i++;
        }

        return new TreeGenRandom( distribution, this.operatorProbabilityDistribution.keySet().toArray( new String[0] ), this.tables );

    }


}
