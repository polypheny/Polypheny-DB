/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.MetadataObserver.Utils;

import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import org.polypheny.db.schemaDiscovery.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class NodeCloner {

    private NodeCloner() {
    }


    public static AbstractNode deepCopy( AbstractNode node ) {
        return copyNode( node );
    }


    private static AbstractNode copyNode( AbstractNode n ) {
        AbstractNode clone;

        if ( n instanceof AttributeNode a ) {
            AttributeNode c = new AttributeNode( a.getType(), a.getName() );
            c.setSelected( a.isSelected() );
            clone = c;
        } else {
            clone = new Node( n.getType(), n.getName() );
        }

        clone.setProperties( new HashMap<>( n.getProperties() ) );

        List<AbstractNode> clonedChildren = new ArrayList<>();
        for ( AbstractNode child : n.getChildren() ) {
            clonedChildren.add( copyNode( child ) );
        }
        clone.setChildren( clonedChildren );

        return clone;
    }

}
