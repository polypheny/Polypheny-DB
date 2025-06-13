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

import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import org.polypheny.db.schemaDiscovery.Node;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class MetaAnnotator {

    private static class PathHelper {

        static Map<String, AbstractNode> collect( AbstractNode node ) {
            Map<String, AbstractNode> map = new HashMap<>();
            traverse( node, node.getName(), map );
            return map;
        }


        private static void traverse(
                AbstractNode n, String path,
                Map<String, AbstractNode> sink ) {
            sink.put( path, n );
            for ( AbstractNode c : n.getChildren() ) {
                traverse( c, path + "/" + c.getName(), sink );
            }
        }


        static Optional<AbstractNode> getNode( AbstractNode root, String path ) {
            String[] seg = path.split( "/" );
            AbstractNode cur = root;
            for ( int i = 1; i < seg.length; i++ ) {
                String s = seg[i];
                cur = cur.getChildren().stream()
                        .filter( n -> n.getName().equals( s ) )
                        .findFirst().orElse( null );
                if ( cur == null ) {
                    return Optional.empty();
                }
            }
            return Optional.of( cur );
        }

    }


    public static AbstractNode annotateTree( AbstractNode oldRoot, AbstractNode newRoot, DiffResult diff ) {
        AbstractNode copyOld = NodeCloner.deepCopy( oldRoot );
        AbstractNode copyNew = NodeCloner.deepCopy( newRoot );

        Map<String, AbstractNode> newMap = PathHelper.collect( copyNew );
        Map<String, AbstractNode> oldMap = PathHelper.collect( copyOld );

        for ( Map.Entry<String, AbstractNode> e : oldMap.entrySet() ) {
            if ( e.getValue() instanceof AttributeNode a && a.isSelected() ) {
                AbstractNode match = newMap.get( e.getKey() );
                if ( match instanceof AttributeNode aNew ) {
                    aNew.setSelected( true );
                }
            }
        }

        diff.getAdded().forEach( p -> PathHelper
                .getNode( copyNew, p )
                .ifPresent( n -> n.addProperty( "diff", DiffType.ADDED ) ) );

        /*diff.getChanged().forEach( p -> PathHelper
                .getNode( copyNew, p )
                .ifPresent( n -> n.addProperty( "diff", DiffType.CHANGED ) ) );*/

        for ( String p : diff.getRemoved() ) {
            if ( newMap.containsKey( p ) ) {
                continue;
            }
            createGhostNode( copyNew, p );
        }

        return copyNew;

    }


    private static void createGhostNode( AbstractNode root, String fullPath ) {
        String[] parts = fullPath.split( "/" );
        AbstractNode current = root;
        StringBuilder curPath = new StringBuilder( root.getName() );

        for ( int i = 1; i < parts.length; i++ ) {
            String segment = parts[i];
            curPath.append( "/" ).append( segment );

            Optional<AbstractNode> opt =
                    current.getChildren().stream()
                            .filter( n -> n.getName().equals( segment ) )
                            .findFirst();

            if ( opt.isPresent() ) {
                current = opt.get();
            } else {
                Node stub = new Node( "ghost", segment );
                if ( i == parts.length - 1 ) {
                    stub.addProperty( "diff", DiffType.REMOVED );
                }
                current.addChild( stub );
                current = stub;
            }

        }
    }

}





