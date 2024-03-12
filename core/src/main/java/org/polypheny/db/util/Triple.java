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

package org.polypheny.db.util;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;

@Value(staticConstructor = "of")
@EqualsAndHashCode
@NonFinal
public class Triple<A, B, C> implements Comparable<Triple<A, B, C>> {

    public A left;
    public B middle;
    public C right;


    public Triple( A left, B middle, C right ) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }


    @Override
    public int compareTo( Triple<A, B, C> o ) {
        //noinspection unchecked
        int c = compare( (Comparable) this.left, (Comparable) o.left );
        if ( c == 0 ) {
            //noinspection unchecked
            c = compare( (Comparable) this.right, (Comparable) o.right );
        }
        return c;
    }


    private static <C extends Comparable<C>> int compare( C c1, C c2 ) {
        if ( c1 == null ) {
            if ( c2 == null ) {
                return 0;
            } else {
                return -1;
            }
        } else if ( c2 == null ) {
            return 1;
        } else {
            return c1.compareTo( c2 );
        }
    }


}
