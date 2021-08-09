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

package org.polypheny.cql.parser;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

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

        stringBuilder.append( booleanOperator.getName() )
                .append( " " );

        modifiers.forEach( ( s, modifier ) -> {
            stringBuilder.append( s )
                    .append( " : " )
                    .append( modifier )
                    .append( " " );
        } );

        return stringBuilder.toString();
    }


    protected interface BooleanOperator {

        public String getName();

    }

    public static enum TableOpsBooleanOperator implements BooleanOperator {
        AND( "and" ),
        OR( "or" );


        private final String booleanOperator;


        TableOpsBooleanOperator( String booleanOperator ) {
            this.booleanOperator = booleanOperator;
        }


        @Override
        public String getName() {
            return this.booleanOperator;
        }

    }


    public static enum ColumnOpsBooleanOperator implements BooleanOperator {
        AND( "and" ),
        OR( "or" ),
        NOT( "not" ),
        PROX( "prox" );


        private final String booleanOperator;


        ColumnOpsBooleanOperator( String booleanOperator ) {
            this.booleanOperator = booleanOperator;
        }


        @Override
        public String getName() {
            return this.booleanOperator;
        }
    }




}
