/*
 * Copyright 2019-2023 The Polypheny Project
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
 * Packaging of information used for comparing in {@link Filter}.
 */
public class Relation {

    public final Comparator comparator;
    public final Map<String, Modifier> modifiers;


    public Relation( Comparator comparator ) {
        this.comparator = comparator;
        this.modifiers = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );
    }


    public Relation( Comparator comparator, Map<String, Modifier> modifiers ) {
        this.comparator = comparator;
        this.modifiers = modifiers;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        modifiers.forEach( ( modifierName, modifier ) -> stringBuilder.append( " / " )
                .append( modifierName )
                .append( " " )
                .append( modifier.comparator.toString() )
                .append( " " )
                .append( modifier.modifierValue ) );

        stringBuilder.append( " " );

        return comparator.toString() + stringBuilder;
    }

}
