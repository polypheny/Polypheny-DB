/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Predicate2;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyPath;


/** Chooses one candidate per endpoint pair after applying the enclosing MATCH predicates. */
public final class CypherShortestPathRows {

    private CypherShortestPathRows() {
    }


    public static Enumerable<PolyValue[]> join( Enumerable<PolyValue[]> left, Enumerable<PolyValue[]> right,
            Predicate2<PolyValue[], PolyValue[]> condition, boolean optional, int rightSize, int[] shortestColumns, int[] groupColumns ) {
        List<Input> inputs = new ArrayList<>();
        try ( Enumerator<PolyValue[]> rows = left.enumerator() ) {
            while ( rows.moveNext() ) {
                inputs.add( new Input( rows.current().clone(), new LinkedHashMap<>() ) );
            }
        }
        if ( inputs.isEmpty() ) {
            return Linq4j.emptyEnumerable();
        }
        try ( Enumerator<PolyValue[]> rows = right.enumerator() ) {
            while ( rows.moveNext() ) {
                PolyValue[] candidate = rows.current();
                List<Object> key = group( candidate, shortestColumns, groupColumns );
                for ( Input input : inputs ) {
                    if ( condition.apply( input.row(), candidate ) ) {
                        PolyValue[] best = input.paths().get( key );
                        if ( best == null || shorter( candidate, best, shortestColumns ) ) {
                            input.paths().put( key, candidate.clone() );
                        }
                    }
                }
            }
        }

        List<PolyValue[]> output = new ArrayList<>();
        for ( Input input : inputs ) {
            if ( optional && input.paths().isEmpty() ) {
                output.add( Arrays.copyOf( input.row(), input.row().length + rightSize ) );
            }
            input.paths().values().forEach( candidate -> {
                PolyValue[] row = Arrays.copyOf( input.row(), input.row().length + rightSize );
                System.arraycopy( candidate, 0, row, input.row().length, rightSize );
                output.add( row );
            } );
        }
        return Linq4j.asEnumerable( output );
    }


    private static List<Object> group( PolyValue[] row, int[] shortestColumns, int[] groupColumns ) {
        List<Object> key = new ArrayList<>( 2 * shortestColumns.length + groupColumns.length );
        for ( int index : shortestColumns ) {
            PolyPath path = row[index].asPath();
            key.add( path.getNodes().get( 0 ).id );
            key.add( path.getNodes().get( path.getNodes().size() - 1 ).id );
        }
        for ( int index : groupColumns ) {
            PolyValue value = row[index];
            if ( value instanceof PolyPath path ) {
                // An ordinary pattern can match the same physical path at different segment boundaries.
                List<PolyString> identity = path.getPath().stream().map( element -> element.id ).toList();
                key.add( List.of( identity, path.getMatchNodeOffsets() ) );
            } else if ( value instanceof GraphPropertyHolder element ) {
                key.add( element.id );
            } else {
                key.add( value );
            }
        }
        return key;
    }


    private static boolean shorter( PolyValue[] candidate, PolyValue[] current, int[] shortestColumns ) {
        for ( int index : shortestColumns ) {
            int cmp = Integer.compare( candidate[index].asPath().getEdges().size(), current[index].asPath().getEdges().size() );
            if ( cmp != 0 ) {
                return cmp < 0;
            }
        }
        return false;
    }


    private record Input( PolyValue[] row, Map<List<Object>, PolyValue[]> paths ) {

    }
}
