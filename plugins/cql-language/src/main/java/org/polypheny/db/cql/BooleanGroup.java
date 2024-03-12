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

package org.polypheny.db.cql;

import java.util.Map;
import java.util.TreeMap;


/**
 * Packaging {@link BooleanOperator} and corresponding {@link Modifier}s.
 *
 * @param <E> Type of the boolean operator.
 */
public class BooleanGroup<E extends BooleanGroup.BooleanOperator> {

    public final E booleanOperator;
    public final Map<String, Modifier> modifiers;


    public BooleanGroup( final E booleanOperator ) {
        this.booleanOperator = booleanOperator;
        this.modifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
    }


    public BooleanGroup( final E booleanOperator, final Map<String, Modifier> modifiers ) {
        this.booleanOperator = booleanOperator;
        this.modifiers = modifiers;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append( booleanOperator.getName() ).append( " " );

        modifiers.forEach( ( s, modifier ) -> stringBuilder.append( s )
                .append( " : " )
                .append( modifier )
                .append( " " ) );

        return stringBuilder.toString();
    }


    /**
     * Boolean operators that are to be used with tables.
     */
    public enum EntityOpsBooleanOperator implements BooleanOperator {
        AND( "and" ),
        OR( "or" );


        private final String booleanOperator;


        EntityOpsBooleanOperator( String booleanOperator ) {
            this.booleanOperator = booleanOperator;
        }


        @Override
        public String getName() {
            return this.booleanOperator;
        }

    }


    /**
     * Boolean operators that are to be used with columns.
     */
    public enum FieldOpsBooleanOperator implements BooleanOperator {
        AND( "and" ),
        OR( "or" ),
        NOT( "not" ),
        PROX( "prox" );


        private final String booleanOperator;


        FieldOpsBooleanOperator( String booleanOperator ) {
            this.booleanOperator = booleanOperator;
        }


        @Override
        public String getName() {
            return this.booleanOperator;
        }

    }


    protected interface BooleanOperator {

        String getName();

    }

}
