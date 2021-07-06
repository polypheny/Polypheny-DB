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
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

public class CqlQuery {

    public final QueryNode root;
    public final ArrayList<SortSpecification> sortSpecs;
    public final HashSet<String> indices;
    public final HashMap<String, String> prefixURIPairs;


    public CqlQuery( QueryNode root, ArrayList<SortSpecification> sortSpecs,
            HashSet<String> indices, HashMap<String, String> prefixURIPairs ) {
        this.root = root;
        this.sortSpecs = sortSpecs;
        this.indices = indices;
        this.prefixURIPairs = prefixURIPairs;
    }


    @Override
    public String toString() {
        return "> " + prefixURIPairs.keySet().stream().map( k -> k + "=" + prefixURIPairs.get( k ) ).collect( Collectors.joining( " " ) ) + "\n" +
                queryString( root ) + "\nsortby " +
                sortSpecs.stream().map( Object::toString ).collect( Collectors.joining( " " ) ) + "\n"
                + "[ " + indices.stream().map( Object::toString ).collect( Collectors.joining( ", " ) ) + " ]";
    }


    private String queryString( QueryNode root ) {
        if ( root == null ) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        if ( root.isLeaf() ) {
            stringBuilder.append( root ).append( " " );
            return stringBuilder.toString();
        } else {
            stringBuilder.append( "( " ).append( queryString( root.left ) ).append( " " ).append( root ).append( " " ).append( queryString( root.right ) ).append( " )" );
            return stringBuilder.toString();
        }
    }

}
