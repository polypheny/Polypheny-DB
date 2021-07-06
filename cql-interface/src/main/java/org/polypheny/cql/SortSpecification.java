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

package org.polypheny.cql;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class SortSpecification {

    public final String index;
    public final ArrayList<Modifier> modifiers;


    public SortSpecification( String index, ArrayList<Modifier> modifiers ) {
        this.index = index;
        this.modifiers = modifiers;
    }


    public SortSpecification( String index ) {
        this.index = index;
        this.modifiers = new ArrayList<>();
    }


    @Override
    public String toString() {
        return index + ( modifiers.isEmpty() ? " " : "/ " +
                modifiers.stream().map( Object::toString ).collect( Collectors.joining( " " ) ) );
    }

}
