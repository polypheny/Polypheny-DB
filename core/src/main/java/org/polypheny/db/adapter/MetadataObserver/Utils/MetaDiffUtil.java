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

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MetaDiffUtil {

    @Getter
    @AllArgsConstructor
    public static class DiffResult {

        private final Set<String> added;
        private final Set<String> removed;
        private final Set<String> changed;


        @Override
        public String toString() {
            return "DiffResult{" +
                    "added=" + added +
                    ", removed=" + removed +
                    ", changed=" + changed +
                    '}';
        }

    }


    @EqualsAndHashCode
    @RequiredArgsConstructor(staticName = "of")
    // Instead of comparing every variable in a node, a hash (fingerprint) is created for every node.
    // Used for comparison.
    public static class Fingerprint {

        private final String type;
        private final boolean selected;
        private final int propertiesHash;


        static Fingerprint of( AbstractNode n ) {
            boolean sel = (n instanceof AttributeNode) && ((AttributeNode) n).isSelected();
            return Fingerprint.of( n.getType(), sel, Objects.hashCode( n.getProperties() ) );
        }

    }


    private MetaDiffUtil() {
    }


    public static DiffResult diff( AbstractNode oldRoot, AbstractNode newRoot ) {
        Map<String, Fingerprint> oldMap = new HashMap<>();
        Map<String, Fingerprint> newMap = new HashMap<>();

        collect( oldRoot, "", oldMap );
        collect( newRoot, "", newMap );

        Set<String> added = new HashSet<>( newMap.keySet() );
        added.removeAll( oldMap.keySet() );

        Set<String> removed = new HashSet<>( oldMap.keySet() );
        removed.removeAll( newMap.keySet() );

        Set<String> changed = new HashSet<>();
        for ( String key : oldMap.keySet() ) {
            if ( newMap.containsKey( key ) &&
                    !oldMap.get( key ).equals( newMap.get( key ) ) ) {
                changed.add( key );
            }
        }

        return new DiffResult( added, removed, Collections.emptySet() );

    }


    private static void collect( AbstractNode node, String parentPath, Map<String, Fingerprint> sink ) {
        String path = parentPath.isEmpty() ?
                node.getName() :
                parentPath + "/" + node.getName();

        sink.put( path, Fingerprint.of( node ) );

        for ( AbstractNode child : node.getChildren() ) {
            collect( child, path, sink );
        }
    }


}
