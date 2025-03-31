/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.util;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value(staticConstructor = "of")
@EqualsAndHashCode
@NonFinal
public class Quadruple<A, B, C, D> implements Comparable<Quadruple<A, B, C, D>> {

    public A a;
    public B b;
    public C c;
    public D d;


    public Quadruple( A a, B b, C c, D d ) {
        this.a = a;
        this.b = b;
        this.c = c;
        this.d = d;
    }


    @Override
    public int compareTo( Quadruple<A, B, C, D> o ) {
        //noinspection unchecked
        int cmp = compare( (Comparable) this.a, (Comparable) o.a );
        if ( cmp == 0 ) {
            //noinspection unchecked
            cmp = compare( (Comparable) this.b, (Comparable) o.b );
            if ( cmp == 0 ) {
                //noinspection unchecked
                cmp = compare( (Comparable) this.c, (Comparable) o.c );
                if ( cmp == 0 ) {
                    //noinspection unchecked
                    cmp = compare( (Comparable) this.d, (Comparable) o.d );
                }
            }
        }
        return cmp;
    }


    private static <C extends Comparable<C>> int compare( C c1, C c2 ) {
        if ( c1 == null ) {
            return (c2 == null) ? 0 : -1;
        } else if ( c2 == null ) {
            return 1;
        } else {
            return c1.compareTo( c2 );
        }
    }

}
