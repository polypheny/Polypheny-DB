/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j.util;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.util.Pair;

public class TypeFrame {

    final List<Pair<String, String>> parentProperty;


    TypeFrame( List<Pair<String, String>> parentProperty ) {
        this.parentProperty = parentProperty;
    }


    public static TypeFrame nodeFrame( String nodeName ) {
        return new TypeFrame( List.of( Pair.of( nodeName, null ) ) );
    }


    public static TypeFrame nodesFrame( List<String> nodeNames ) {
        return new TypeFrame( nodeNames.stream().map( n -> Pair.of( n, (String) null ) ).collect( Collectors.toList() ) );
    }


    public static TypeFrame entryFrame( String entryName ) {
        return new TypeFrame( List.of( Pair.of( null, entryName ) ) );
    }


    public static TypeFrame entriesFrame( List<String> names ) {
        return new TypeFrame( names.stream().map( n -> Pair.of( (String) null, n ) ).collect( Collectors.toList() ) );
    }


    public int size() {
        return parentProperty.size();
    }


    public boolean inNodeForm() {
        return parentProperty.size() == 1 && parentProperty.get( 0 ).right == null;
    }


    public String getNodeName() {
        return parentProperty.get( 0 ).left;
    }

}
