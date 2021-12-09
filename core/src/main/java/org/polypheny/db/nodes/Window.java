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

package org.polypheny.db.nodes;

public interface Window {

    /**
     * Returns whether an expression represents the "CURRENT ROW" bound.
     */
    static boolean isCurrentRow( Node node ) {
        return (node instanceof Literal) && ((Literal) node).symbolValue( Bound.class ) == Bound.CURRENT_ROW;
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED PRECEDING" bound.
     */
    static boolean isUnboundedPreceding( Node node ) {
        return (node instanceof Literal) && ((Literal) node).symbolValue( Bound.class ) == Bound.UNBOUNDED_PRECEDING;
    }

    /**
     * Returns whether an expression represents the "UNBOUNDED FOLLOWING" bound.
     */
    static boolean isUnboundedFollowing( Node node ) {
        return (node instanceof Literal) && ((Literal) node).symbolValue( Bound.class ) == Bound.UNBOUNDED_FOLLOWING;
    }

    /**
     * An enumeration of types of bounds in a window: <code>CURRENT ROW</code>, <code>UNBOUNDED PRECEDING</code>, and <code>UNBOUNDED FOLLOWING</code>.
     */
    enum Bound {
        CURRENT_ROW( "CURRENT ROW" ),
        UNBOUNDED_PRECEDING( "UNBOUNDED PRECEDING" ),
        UNBOUNDED_FOLLOWING( "UNBOUNDED FOLLOWING" );

        private final String sql;


        Bound( String sql ) {
            this.sql = sql;
        }


        public String toString() {
            return sql;
        }

    }

}
