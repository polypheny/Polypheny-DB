/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb.util;

import java.util.Map;
import org.polypheny.db.util.Pair;

public class MongoPair<T1, T2, T3> extends Pair<T1, T2> {

    T3 op;


    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public MongoPair( T1 left, T2 right, T3 op ) {
        super( left, right );
        this.op = op;
    }


    public Map.Entry<T3, Map.Entry<T1, T2>> asEntry() {
        return new Pair<>( op, new Pair<>( left, right ) );
    }


    @Override
    public boolean equals( Object obj ) {
        if ( obj instanceof MongoPair ) {
            MongoPair<?, ?, ?> other = (MongoPair<?, ?, ?>) obj;

            return super.equals( other )
                    && ((other.op == null && op == null) || (other.op != null && other.op.equals( op )));

        }

        return false;
    }


    @Override
    public String toString() {
        if ( op == null ) {
            return super.toString();
        } else {
            return "<" + op + ":" + left + "," + right + ">";
        }
    }


    @Override
    public int hashCode() {
        return op == null ? "".hashCode() ^ super.hashCode() : op.hashCode() ^ super.hashCode();
    }

}
