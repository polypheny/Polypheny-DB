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

package org.polypheny.db.adaptimizer.models.deeplearning;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.util.Pair;

@Slf4j
public class Vectorizer {
    public static final int VECTOR_LENGTH = (int) Math.pow( 2, 7 ) * new TempVectorObject().asList().size();

    public static Vec vectorize( String physicalQuery ) {
        String[] split = physicalQuery.split( "\n" );

        ArrayList<TempVectorObject> tmps = new ArrayList<>();
        for ( String line : split ) {
            TempVectorObject tmp = new TempVectorObject();

            line = line.substring( 0, line.indexOf( "subset" ) ) + line.substring( line.indexOf( "], " ) + 3 );
            line = line.substring( 0, line.length() - 1 );
            String[] split2 = line.split( "\\(" );

            tmp.op = split2[ 0 ].strip().hashCode();
            tmp.depth = (split2[ 0 ].lastIndexOf( " " ) - 1) / 2d;

            if ( split2[ 1 ].startsWith( "table" )) {
                tmp.table = split2[ 1 ].hashCode();
            } else if (split2[ 1 ].startsWith( "sort" )) {
                String[] split3 = split2[ 1 ].split( ", " );
                tmp.sort = split3[ 0 ].split( "=" )[ 1 ].hashCode()
                        +  split3[ 1 ].split( "=" )[ 1 ].hashCode();
            } else if (split2[ 1 ].startsWith( "all" )) {
                tmp.setOp = split2[ 1 ].split( "=" )[ 1 ].hashCode();
                tmp.isBinary = true;
            } else if (split2[ 1 ].startsWith( "condition" )) {
                if (split2[ 0 ].toLowerCase().contains( "join" )) {
                    String[] split3 = split2[ 1 ].split( ", " );
                    tmp.join = split3[ 0 ].split( "=" )[ 1 ].hashCode()
                            +  split3[ 1 ].split( "=" )[ 1 ].hashCode();
                    tmp.isBinary = true;
                } else {
                    tmp.filter = split2[ 1 ].split( "=" )[ 1 ].hashCode();
                }
            } else if ( split2[ 0 ].toLowerCase().contains( "project" ) ) {
                String[] split3 = split2[ 1 ].split( ", " );
                double sum = 0;
                for ( String s : split3 ) {
                    sum += s.split( "=" )[ 0 ].hashCode();
                }
                tmp.project = sum;
            }
            tmps.add( tmp );
        }

        double[] values = new double[ VECTOR_LENGTH ];
        recursiveFill( 1, values, 0, tmps );
        return new Vec( values );
    }

    private static void recursiveFill( int idx, double[] values, int idx2, ArrayList<TempVectorObject> tmps ) {
        fillAt( ( idx - 1 ) * 8 + 1, values, tmps.get( idx2 ) );
        if ( tmps.get( idx2 ).isBinary() ) {
            Pair<Integer, Integer> children = getNextTwo( idx2, tmps );
            recursiveFill( idx * 2, values, children.left, tmps );
            recursiveFill( idx * 2 + 1 , values, children.right, tmps );
        } else if ( tmps.get( idx2 ).table == 0 ) {
            recursiveFill( idx * 2, values, idx2 + 1, tmps );
        }
    }

    private static Pair<Integer, Integer> getNextTwo( int idx2, ArrayList<TempVectorObject> tmps ) {
        int searchDepth = (int) (tmps.get( idx2 ).depth + 1);
        int k = idx2 + 2;
        while ( (int) tmps.get( k ).depth != searchDepth ) {
            k++;
        }
        return new Pair<>( idx2 + 1, k );
    }

    private static void fillAt( int idx, double[] values, TempVectorObject tmp ) {
        List<Double> xs = tmp.asList();
        for ( int j = 0; j < xs.size(); j++ ) {
            values[ idx + j ] = xs.get( j );
        }
    }

    @Getter
    @Setter
    private static class TempVectorObject {
        boolean isBinary = false;

        double op = 0;
        double depth = 0;
        double table = 0;
        double sort = 0;
        double setOp = 0;
        double filter = 0;
        double project = 0;
        double join = 0;

        public List<Double> asList() {
            return new ArrayList<>( List.of(
                    op,
                    depth,
                    table,
                    sort,
                    setOp,
                    filter,
                    project,
                    join
            ) );
        }

    }

}
