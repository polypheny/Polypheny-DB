/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.functions.util;

import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;

/**
 * Enumerates over the cartesian product of the given lists, returning a comparable list for each row.
 *
 * @param <E> element type
 */
public class ProductPolyListEnumerator<E extends PolyValue> extends CartesianProductEnumerator<PolyList<E>, PolyList<E>> {

    final E[] flatElements;
    final PolyList<E> list;
    private final boolean withOrdinality;
    private int ordinality;


    public ProductPolyListEnumerator( List<Enumerator<PolyList<E>>> enumerators, int fieldCount, boolean withOrdinality ) {
        super( enumerators );
        this.withOrdinality = withOrdinality;
        flatElements = (E[]) new PolyValue[fieldCount];
        list = PolyList.of( flatElements );
    }


    @Override
    public PolyList<E> current() {
        int i = 0;
        for ( PolyValue element : elements ) {
            PolyList<E> list = element.asList();
            PolyValue[] a = list.toArray( PolyValue[]::new );
            System.arraycopy( a, 0, flatElements, i, a.length );
            i += a.length;
        }
        if ( withOrdinality ) {
            flatElements[i] = (E) PolyInteger.of( ++ordinality ); // 1-based
        }
        return PolyList.of( flatElements );
    }

}
