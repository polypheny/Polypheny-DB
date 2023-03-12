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

package org.polypheny.db.polyfier.schemas;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.polypheny.db.polyfier.core.PolyfierException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Slf4j
@Builder
@Getter(AccessLevel.PRIVATE)
public class SchemaTemplate implements Serializable, SeededClass {
    private static final List<PolyType> SUPPORTED_TYPES = List.of(
            PolyType.FLOAT,
            PolyType.DOUBLE,
            PolyType.TINYINT,
            PolyType.SMALLINT,
            PolyType.INTEGER,
            PolyType.VARCHAR
    );
    private static final List<PolyType> SUPPORTED_ISOLATED_TYPES = List.of(
            PolyType.BOOLEAN,
            PolyType.DATE,
            PolyType.TIME,
            PolyType.TIMESTAMP,
            PolyType.VARCHAR,
            PolyType.FLOAT,
            PolyType.DOUBLE
    );

    @Default
    private int colCounter = 0;

    @Default
    private int tableCounter = 0;

    private Random random;

    private float referenceProbability;

    private double meanTables;

    private double sigmaTables;

    private double meanAttributes;

    private double sigmaAttributes;

    private double meanReferences;

    private double sigmaReferences;


    public String nextSchema() {
        return "Schema_" + RandomStringUtils.randomAlphabetic( 5 );
    }

    public int nextNrOfTables() {
        return (int) this.nextGaussian( this.getMeanTables(), this.getSigmaTables() );
    }

    public int nextNrOfAttributes() {
        return (int) this.nextGaussian( this.getMeanAttributes(), this.getSigmaAttributes() );
    }

    public String nextTable() {
        return "Table_" + tableCounter++;
    }

    public String nextColumnName() {
        return "Col_" + colCounter++;
    }

    public PolyType nextPolyType() {
        return new Choice<PolyType>().nextOf( random, SUPPORTED_TYPES );
    }

    public PolyType nextIsolatedPolyType() {
        return new Choice<PolyType>().nextOf( random, SUPPORTED_ISOLATED_TYPES );
    }

    public boolean nextReference( float perColumn ) {
        return this.random.nextFloat() < ( this.referenceProbability * perColumn );
    }

    public int getNextPrimaryKeySize( int j ) {
        return this.random.nextInt( j - 1 ) + 1;
    }
    public String nextConstraint( TableNode tableNode ) {
        return tableNode.tableName  + "_pu" + RandomStringUtils.randomAlphabetic( 5 );
    }

    public String nextConstraint( Pair<ColumnNode, ColumnNode> foreignKey ) {
        return foreignKey.left.columnName + "_" + foreignKey.right.columnName + "_fk";
    }


    public double nextGaussian( double mean, double sigma ) {
        double gaussian = this.random.nextGaussian();
        return  mean + gaussian * sigma;
    }

    public boolean nextBoolean() {
        return this.random.nextBoolean();
    }

    @Override
    public Random getRnd() {
        return this.random;
    }

    public void shuffle( List<ColumnNode> columns ) {
        shuffleAux( columns, 10 );
    }

    private void shuffleAux( List<ColumnNode> xs, int x ) {
        if ( x == 0 ) {
            return;
        }
        for ( int i = 0; i < xs.size(); i++ ) {
            for ( int j = 0; j < xs.size(); j++ ) {
                if ( random.nextBoolean() ) {
                    ColumnNode tmp = xs.get( i );
                    xs.set( i, xs.get( j ) );
                    xs.set( j, tmp );
                }
            }
        }
        shuffleAux( xs, x - 1 );
    }

    public ColumnNode getNextPrimaryKey( TableNode tableNode ) {
        List<ColumnNode> possiblePrimaryKeys = tableNode.columnNodes.stream().filter( columnNode -> SUPPORTED_TYPES.contains( columnNode.polyType ) ).collect( Collectors.toList() );
        if ( possiblePrimaryKeys.isEmpty() ) {
            possiblePrimaryKeys = tableNode.columnNodes.stream().filter( columnNode ->
                    columnNode.referencedBy == null && columnNode.references == null ).collect( Collectors.toList());
        }
        if ( possiblePrimaryKeys.isEmpty() ) {
            throw new PolyfierException( "Could not find a primary Key", new RuntimeException() );
        }
        ColumnNode columnNode = new Choice<ColumnNode>().nextOf( random, possiblePrimaryKeys );

        columnNode.nullable = false;

        return columnNode;
    }

    /**
     * Auxiliary Class for readability.
     */
    private static class Choice<T> {
        public T nextOf( Random random, List<T> list ) {
            return list.get( random.nextInt( list.size() ) );
        }
    }

}
