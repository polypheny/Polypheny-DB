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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.polypheny.cql.cql2rel.Index;
import org.polypheny.cql.utils.Tree.NodeType;
import org.polypheny.cql.utils.Tree.TraversalType;

public class CqlQuery {

    public final QueryNode tableOperations;
    public final QueryNode filters;
    public final ArrayList<SortSpecification> sortSpecs;
    public final HashMap<String, Index> indexMapping;


    public CqlQuery( QueryNode tableOperations, QueryNode filters, ArrayList<SortSpecification> sortSpecs, HashMap<String, Index> indexMapping ) {
        this.tableOperations = tableOperations;
        this.filters = filters;
        this.sortSpecs = sortSpecs;
        this.indexMapping = indexMapping;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if ( tableOperations != null ) {
            stringBuilder.append( queryString( tableOperations ) );
        }
        if ( filters != null ) {
            stringBuilder.append( queryString( filters ) );
        }
        if ( sortSpecs != null ) {
            stringBuilder.append( "\nsortby " )
                    .append( sortSpecs.stream().map( Object::toString ).collect( Collectors.joining( " " ) ) )
                    .append( "\n" );
        }
        return stringBuilder.toString();
    }


    private String queryString( QueryNode root ) {
        if ( root == null ) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();

        root.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, data ) -> {
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                stringBuilder.append( treeNode );
            }
            return true;
        } );

        return stringBuilder.toString();
    }

}
