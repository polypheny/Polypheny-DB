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

package org.polypheny.db.runtime.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;


@Deterministic
@Slf4j
public class CypherFunctions {

    private CypherFunctions() {
        // empty on purpose
    }


    /**
     * Matches all suitable paths for the given graph.
     *
     * @param graph the full graph
     * @param comp the reference path, which is matched
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyPath> pathMatch( PolyGraph graph, PolyPath comp ) {
        return Linq4j.asEnumerable( graph.extract( comp ) );
    }


    /**
     * Matches all suitable nodes for the given graph.
     *
     * @param graph the full graph
     * @param node the node to match
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> nodeMatch( PolyGraph graph, PolyNode node ) {
        return Linq4j.asEnumerable( graph.extract( node ) );
    }


    /**
     * Extracts all nodes as {@link Enumerable} of the given graph.
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> nodeExtract( PolyGraph graph ) {
        return Linq4j.asEnumerable( graph.getNodes().values() );
    }


    /**
     * Creates a new graph from the provided nodes and edges.
     *
     * @param nodes collection of nodes
     * @param edges collection of edges
     */
    @SuppressWarnings("unused")
    public static Enumerable<?> toGraph( Enumerable<PolyNode> nodes, Enumerable<PolyEdge> edges ) {
        PolyMap<String, PolyNode> ns = new PolyMap<>();
        for ( PolyNode node : nodes ) {
            ns.put( node.id, node );
        }

        PolyMap<String, PolyEdge> es = new PolyMap<>();
        for ( PolyEdge edge : edges ) {
            es.put( edge.id, edge );
        }

        return Linq4j.asEnumerable( List.of( new PolyGraph( ns, es ) ) );
    }


    /**
     * Transforms the relational normalized edges into {@link PolyEdge}s
     *
     * @param edge the normalized edges
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyEdge> toEdge( Enumerable<?> edge ) {
        List<PolyEdge> edges = new ArrayList<>();

        String oldId = null;
        String oldSourceId = null;
        String oldTargetId = null;
        Set<String> oldLabels = new HashSet<>();
        Map<String, Comparable<?>> oldProps = new HashMap<>();

        for ( Object value : edge ) {
            Object[] o = (Object[]) value;
            String id = (String) o[0];
            String label = (String) o[1];
            String sourceId = (String) o[2];
            String targetId = (String) o[3];
            // id is 4
            String key = (String) o[5];
            String val = (String) o[6];

            if ( id != null && !id.equals( oldId ) ) {
                if ( oldId != null ) {
                    edges.add( new PolyEdge( oldId, new PolyDictionary( oldProps ), List.copyOf( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT, null ) );
                }
                oldId = id;
                oldLabels = new HashSet<>();
                oldSourceId = sourceId;
                oldTargetId = targetId;
                oldProps = new HashMap<>();
            }
            oldLabels.add( label );

            if ( key != null ) {
                // id | key | value | source | target
                // 13 | null| null | 12      | 10 ( no key value present )
                oldProps.put( key, val );
            }
        }

        if ( oldId != null ) {
            edges.add( new PolyEdge( oldId, new PolyDictionary( oldProps ), List.copyOf( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT, null ) );
        }

        return Linq4j.asEnumerable( edges );
    }


    /**
     * Transforms the relational normalized edges into {@link PolyEdge}s
     *
     * @param node the normalized nodes
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> toNode( Enumerable<?> node ) {
        List<PolyNode> nodes = new ArrayList<>();

        String oldId = null;
        Set<String> oldLabels = new HashSet<>();
        Map<String, Comparable<?>> oldProps = new HashMap<>();

        for ( Object value : node ) {
            Object[] o = (Object[]) value;
            String id = (String) o[0];
            String label = (String) o[1];
            // id is 2
            String key = (String) o[3];
            String val = (String) o[4];

            if ( id != null && !id.equals( oldId ) ) {
                if ( oldId != null ) {
                    nodes.add( new PolyNode( oldId, new PolyDictionary( oldProps ), List.copyOf( oldLabels ), null ) );
                }
                oldId = id;
                oldLabels = new HashSet<>();
                oldProps = new HashMap<>();
            }
            if ( label != null && !label.equals( "$" ) ) {
                // eventually no labels
                oldLabels.add( label );
            }
            if ( key != null ) {
                // eventually no properties present
                oldProps.put( key, val );
            }
        }

        if ( oldId != null ) {
            nodes.add( new PolyNode( oldId, new PolyDictionary( oldProps ), List.copyOf( oldLabels ), null ) );
        }

        return Linq4j.asEnumerable( nodes );
    }


    /**
     * Returns if the given node has the given key.
     *
     * @param node the target node
     * @param property the key to check
     */
    @SuppressWarnings("unused")
    public static boolean hasProperty( PolyNode node, String property ) {
        return node.properties.containsKey( property );
    }


    /**
     * Returns if the given node has the given label.
     *
     * @param node the target node
     * @param label the label to check
     */
    @SuppressWarnings("unused")
    public static boolean hasLabel( PolyNode node, String label ) {
        return node.labels.contains( label );
    }


    /**
     * Extracts the given variable from a provided path e.g. ()-[e]-(), e -> e
     *
     * @param path the path containing the element with the given variable
     * @param variableName the name of the given variable
     */
    @SuppressWarnings("unused")
    public static GraphObject extractFrom( PolyPath path, String variableName ) {
        return path.get( variableName );
    }


    /**
     * Extracts a given property from a graph element.
     *
     * @param holder the target from which the property is extracted
     * @param key the key of the property to extract
     * @return the property value
     */
    @SuppressWarnings("unused")
    public static String extractProperty( GraphPropertyHolder holder, String key ) {
        if ( holder.getProperties().containsKey( key ) ) {
            return holder.getProperties().get( key ).toString();
        }
        return null;
    }


    @SuppressWarnings("unused")
    public static String extractProperties( GraphPropertyHolder holder ) {
        return holder.getProperties().toString();
    }


    @SuppressWarnings("unused")
    public static String extractId( GraphPropertyHolder holder ) {
        return holder.getId();
    }


    /**
     * Returns a single label of a given graph element.
     *
     * @param holder the target from which the label is extracted
     */
    @SuppressWarnings("unused")
    public static String extractLabel( GraphPropertyHolder holder ) {
        return holder.getLabels().get( 0 );
    }


    /**
     * Returns all labels of a given graph element.
     *
     * @param holder the target from which the labels are extracted
     */
    @SuppressWarnings("unused")
    public static List<String> extractLabels( GraphPropertyHolder holder ) {
        return holder.getLabels();
    }


    /**
     * Transform the object into a list by either wrapping it or leaving it as is if it already is.
     *
     * @param obj the object to transform
     */
    @SuppressWarnings("unused")
    public static List<?> toList( Object obj ) {
        if ( obj == null ) {
            return List.of();
        }
        if ( obj instanceof List ) {
            return (List<?>) obj;
        }
        return List.of( obj );
    }


    /**
     * Replaces the left and right side of the given edge with the provided elements.
     *
     * @param edge the edge to adjust
     * @param left the left element of the edge
     * @param right the right element of the edge
     * @return the adjusted edge, where both elements are replaced
     */
    @SuppressWarnings("unused")
    public static PolyEdge adjustEdge( PolyEdge edge, PolyNode left, PolyNode right ) {
        // If isVariable is true, the node is only a placeholder and the id should not be replaced
        return edge.from( left.isVariable() ? null : left.id, right.isVariable() ? null : right.id );
    }


    /**
     * Sets a property value for a given key.
     * If the key does not exist it gets created, if it does, it gets replaced.
     *
     * @param target the graph element to modify
     * @param key the key of the property to replace
     * @param value the value to replace it by
     * @return the adjusted graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperty( GraphPropertyHolder target, String key, Object value ) {
        target.properties.put( key, value );
        return target;
    }


    /**
     * Sets all given labels for the target graph element.
     *
     * @param target the graph element to modify
     * @param labels the labels to set
     * @param replace if the labels a replaced completely or only added
     * @return the adjusted graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setLabels( GraphPropertyHolder target, List<String> labels, boolean replace ) {
        if ( replace ) {
            target.labels.clear();
        }
        target.setLabels( labels );
        return target;
    }


    /**
     * Sets all given properties for the given graph element.
     *
     * @param target the graph element to modify
     * @param keys the keys, which are set
     * @param values the values for the keys
     * @param replace if the properties are replaced completely or only added and replaced
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperties( GraphPropertyHolder target, List<String> keys, List<Object> values, boolean replace ) {
        if ( replace ) {
            target.properties.clear();
        }

        int i = 0;
        for ( String key : keys ) {
            target.properties.put( key, values.get( i ) );
            i++;
        }
        return target;
    }


    /**
     * Removes the given labels for graph object.
     *
     * @param target the graph element to modify
     * @param labels the labels to remove
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeLabels( GraphPropertyHolder target, List<String> labels ) {
        target.labels.removeAll( labels );
        return target;
    }


    /**
     * Removes a property from a given graph element.
     *
     * @param target the graph element to modify
     * @param key the key of the property to remove
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeProperty( GraphPropertyHolder target, String key ) {
        target.properties.remove( key );
        return target;
    }


}
