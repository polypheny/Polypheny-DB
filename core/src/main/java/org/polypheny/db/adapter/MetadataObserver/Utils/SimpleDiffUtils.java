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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleDiffUtils {

    public static List<SimpleDiff> findAddedNodes( AbstractNode oldNode, AbstractNode newNode ) {
        List<SimpleDiff> diffs = new ArrayList<>();
        findAddedRecursive( oldNode, newNode, "/" + newNode.getName(), diffs );
        return diffs;
    }


    private static void findAddedRecursive(
            AbstractNode oldNode,
            AbstractNode newNode,
            String path,
            List<SimpleDiff> diffs ) {
        Map<String, AbstractNode> oldIndex = indexChildren( oldNode.getChildren() );
        Map<String, AbstractNode> newIndex = indexChildren( newNode.getChildren() );

        for ( Map.Entry<String, AbstractNode> entry : newIndex.entrySet() ) {
            String id = entry.getKey();
            if ( !oldIndex.containsKey( id ) ) {
                String[] parts = id.split( ":", 2 );
                String childType = parts[0];
                String childName = parts[1];
                String childPath = path + "/" + childType + ":" + childName;

                diffs.add( new SimpleDiff(
                        SimpleDiff.Type.NODE_ADDED,
                        childPath,
                        childType,
                        childName
                ) );
                findAddedRecursive(
                        new AbstractNodeStub(),
                        entry.getValue(),
                        childPath,
                        diffs
                );
            } else {
                AbstractNode oldChild = oldIndex.get( id );
                AbstractNode newChild = entry.getValue();
                String childPath = path + "/" + id;
                findAddedRecursive( oldChild, newChild, childPath, diffs );
            }
        }
    }


    /**
     * Baut aus einer Liste von AbstractNode eine Map<"type:name", node>.
     */
    private static Map<String, AbstractNode> indexChildren( List<AbstractNode> children ) {
        Map<String, AbstractNode> map = new HashMap<>();
        if ( children != null ) {
            for ( AbstractNode c : children ) {
                String key = c.getType() + ":" + c.getName();
                map.put( key, c );
            }
        }
        return map;
    }


    /**
     * Leere Platzhalter-Klasse, um leere Subtrees darzustellen
     */
    private static class AbstractNodeStub implements AbstractNode {

        @Override
        public String getType() {
            return "";
        }


        @Override
        public String getName() {
            return "";
        }


        @Override
        public List<AbstractNode> getChildren() {
            return Collections.emptyList();
        }


        @Override
        public Map<String, Object> getProperties() {
            return Collections.emptyMap();
        }


        @Override
        public void addChild( AbstractNode node ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void addProperty( String key, Object value ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setType( String type ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setName( String name ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setChildren( List<AbstractNode> children ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setProperties( Map<String, Object> properties ) {
            throw new UnsupportedOperationException();
        }

    }

}
