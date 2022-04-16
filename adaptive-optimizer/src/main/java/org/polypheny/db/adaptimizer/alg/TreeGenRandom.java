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

import java.util.HashMap;
import java.util.Random;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Random object for Tree Generation.
 */
public class TreeGenRandom implements RandomTreeOptions {
    private final Random random;
    private final String[] operators;
    private final int[] probabilityDistribution;
    private final String[] tableNames;
    private final HashMap<String, String[]> columnNames;
    private final HashMap<String, PolyType> columnTypes;

    public TreeGenRandom(
        int[] probabilityDistribution,
        String[] operators,
        HashMap<String, HashMap<String, PolyType>> tables
    ) {
        this.random = new Random();
        this.operators = operators;
        this.probabilityDistribution = probabilityDistribution;

        this.tableNames = tables.keySet().toArray( String[]::new );

        this.columnNames = new HashMap<>();
        tables.forEach( ( k, v ) -> this.columnNames.put( k, v.keySet().toArray( String[]::new ) ) );

        this.columnTypes = new HashMap<>();
        tables.keySet().stream().map( tables::get ).forEach( this.columnTypes::putAll );

    }

    public String nextOperatorType() {
        int i = this.random.nextInt( this.probabilityDistribution.length );
        return operators[ probabilityDistribution[ i ] ];
    }

    public String nextTable() {
        int i = this.random.nextInt( this.tableNames.length );
        return tableNames[i];
    }

    public String nextColumn( String tableName ) {
        int i = this.random.nextInt( this.columnNames.get( tableName ).length );
        return this.columnNames.get( tableName )[i];
    }

    private String nextColumn( String tableName, PolyType type ) {
        int i = this.random.nextInt( this.columnNames.get( tableName ).length );
        return this.columnNames.get( tableName )[i];
    }

    public Pair<String, String> nextColumnPair( String tableLeft, String tableRight ) {
        return new Pair<>( this.nextColumn( tableLeft ), this.nextColumn( tableRight ) );
    }


}
