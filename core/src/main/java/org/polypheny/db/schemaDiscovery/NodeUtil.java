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

package org.polypheny.db.schemaDiscovery;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.MetadataObserver.PublisherManager.ChangeStatus;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
public final class NodeUtil {

    private static final String NORMALIZED_SEPARATOR = ".";


    private static String normalizePath( String rawPath ) {
        return rawPath.replace( "/", NORMALIZED_SEPARATOR )
                .replace( "\\", NORMALIZED_SEPARATOR );
    }


    private NodeUtil() {
    }


    public static Set<String> collectSelecedAttributePaths( AbstractNode root ) {
        Set<String> selected = new HashSet<>();
        if ( root == null ) {
            return selected;
        }
        Deque<String> path = new ArrayDeque<>();
        traverse( root, path, selected );
        return selected;
    }


    private static void traverse( AbstractNode node, Deque<String> path, Set<String> acc ) {
        path.addLast( node.getName() );
        if ( node instanceof AttributeNode attr && attr.isSelected() ) {
            acc.add( String.join( ".", path ) );
        }

        for ( AbstractNode child : node.getChildren() ) {
            traverse( child, path, acc );
        }

        path.removeLast();
    }


    public static void unmarkSelectedAttributes( AbstractNode metadataRoot, List<String> pathsToUnmark ) {

        List<List<String>> attributePaths = new ArrayList<>();

        for ( String path : pathsToUnmark ) {
            String cleanPath = path.replaceFirst( "^.*/", "" ).trim();

            List<String> segments = Arrays.asList( cleanPath.split( "\\." ) );

            if ( !segments.isEmpty() && segments.get( 0 ).equals( metadataRoot.getName() ) ) {
                segments = segments.subList( 1, segments.size() );
            }

            attributePaths.add( segments );
        }

        for ( List<String> pathSegments : attributePaths ) {
            AbstractNode current = metadataRoot;

            for ( int i = 0; i < pathSegments.size(); i++ ) {
                String segment = pathSegments.get( i );

                if ( i == pathSegments.size() - 1 ) {
                    Optional<AbstractNode> attrNodeOpt = current.getChildren().stream()
                            .filter( c -> c instanceof AttributeNode && segment.equals( c.getName() ) )
                            .findFirst();

                    if ( attrNodeOpt.isPresent() ) {
                        ((AttributeNode) attrNodeOpt.get()).setSelected( false );
                    }
                } else {
                    Optional<AbstractNode> childOpt = current.getChildren().stream()
                            .filter( c -> segment.equals( c.getName() ) )
                            .findFirst();

                    if ( childOpt.isPresent() ) {
                        current = childOpt.get();
                    } else {
                        break;
                    }
                }
            }
        }
    }


    public static ChangeStatus evaluateStatus( DiffResult diff, AbstractNode oldRoot ) {
        if ( (diff.getAdded().isEmpty()) && (diff.getRemoved().isEmpty()) ) {
            return ChangeStatus.OK;
        }

        Set<String> selected = collectSelecedAttributePaths( oldRoot );
        for ( String removedRaw : diff.getRemoved() ) {
            String removed = normalizePath( removedRaw );
            for ( String selectedRaw : selected ) {
                String selectedNorm = normalizePath( selectedRaw );
                if ( removed.equals( selectedNorm ) ||
                        selectedNorm.startsWith( removed + NORMALIZED_SEPARATOR ) ||
                        removed.startsWith( selectedNorm + NORMALIZED_SEPARATOR ) ) {
                    return ChangeStatus.CRITICAL;
                }
            }
        }
        return ChangeStatus.WARNING;

    }


}
