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
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;


/** Eagerly executes the updates of a Cypher FOREACH in list order. */
public final class CypherForeachRows {

    private CypherForeachRows() {
    }


    public static Enumerable<PolyValue[]> execute( Enumerable<PolyValue[]> source, Function1<PolyValue[], Enumerable<PolyValue[]>> body,
            boolean countOnly ) {
        List<PolyValue[]> rows = new ArrayList<>();
        long writes = 0;
        // Close scans before writing. Rebind graph aliases to the same live node or relationship value.
        try ( Enumerator<PolyValue[]> input = Functions.materializeGraphRows( source ).enumerator() ) {
            while ( input.moveNext() ) {
                PolyValue[] row = input.current();
                int listIndex = row.length - 1;
                PolyValue values = row[listIndex];
                if ( values != null && !values.isNull() ) {
                    if ( !(values instanceof PolyList<?> items) ) {
                        throw new GenericRuntimeException( "FOREACH requires a list" );
                    }
                    PolyValue[] bindings = row.clone();
                    for ( PolyValue item : items ) {
                        bindings[listIndex] = item;
                        try ( Enumerator<PolyValue[]> updates = body.apply( bindings ).enumerator() ) {
                            while ( updates.moveNext() ) {
                                if ( countOnly ) {
                                    writes += updates.current()[0].asNumber().longValue();
                                }
                            }
                        }
                    }
                }
                if ( !countOnly ) {
                    rows.add( Arrays.copyOf( row, listIndex ) );
                }
            }
        }
        if ( countOnly ) {
            rows.add( new PolyValue[]{ PolyLong.of( writes ) } );
        }
        return Linq4j.asEnumerable( rows );
    }
}
