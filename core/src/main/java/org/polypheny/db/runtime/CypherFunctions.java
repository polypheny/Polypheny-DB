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

package org.polypheny.db.runtime;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Function0;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;

@Deterministic
@Slf4j
public class CypherFunctions {


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
        // if isVariable is true, the node is only a placeholder and the id should not be replaced
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


    /**
     * Splits a graph modify into a normalized update for relational database.
     *
     * @param context the dataContext for the update operation
     * @param enumerables holding the update enumrables for all needed normalized modify operations
     * @param order the order of modify operations
     * @param operation the operation type to perform
     * @return the result of the modify operation
     */
    @SuppressWarnings("unused")
    public static Enumerable<?> sendGraphModifies( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order, Operation operation ) {
        if ( operation == Operation.DELETE ) {
            return sendDeletes( context, enumerables, order );
        }
        if ( operation == Operation.UPDATE ) {
            return sendUpdates( context, enumerables, order );
        }

        int i = 0;
        Map<Long, Object> values = context.getParameterValues().get( 0 );
        Map<Long, AlgDataType> typeBackup = context.getParameterTypes();
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, 36 );
        AlgDataType labelType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );
        AlgDataType valueType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );

        context.resetParameterValues();
        Enumerator<?> enumerable;
        for ( PolyType polyType : order ) {
            if ( polyType == PolyType.NODE ) {
                PolyNode node = (PolyNode) values.get( (long) i );

                // node table insert
                nodeTableInsert( context, enumerables, 2 * i, idType, labelType, node );

                // node property table insert
                nodePropertiesTableInsert( context, enumerables, 2 * i + 1, idType, labelType, valueType, node );

            } else if ( polyType == PolyType.EDGE ) {
                PolyEdge edge = (PolyEdge) values.get( (long) i );

                // edge table insert
                edgeTableInsert( context, enumerables, 2 * i, idType, labelType, edge );

                // edge property table insert
                edgePropertiesTableInsert( context, enumerables, 2 * i + 1, idType, labelType, valueType, edge );

            }

            i++;
        }
        // restore original context
        context.resetParameterValues();
        context.setParameterValues( List.of( values ) );
        context.setParameterTypes( typeBackup );

        return Linq4j.singletonEnumerable( 1 );
    }


    /**
     * Modify operation, which inserts edge properties.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param edge the edge for which the properties are inserted
     */
    private static void edgePropertiesTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, AlgDataType valueType, PolyEdge edge ) {
        if ( !edge.properties.isEmpty() ) {
            context.addParameterValues( 0, idType, Collections.nCopies( edge.properties.size(), edge.id ) );
            context.addParameterValues( 1, labelType, new ArrayList<>( edge.properties.keySet() ) );
            context.addParameterValues( 2, valueType, new ArrayList<>( edge.properties.values().stream().map( Object::toString ).collect( Collectors.toList() ) ) );
            drainInserts( enumerables.get( i ), edge.labels.size() );
            context.resetParameterValues();
        }
    }


    /**
     * Modify operation, which inserts edge id and key.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param edge the edge for which the properties are inserted
     */
    private static void edgeTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, PolyEdge edge ) {
        context.addParameterValues( 0, idType, Collections.nCopies( edge.labels.size(), edge.id ) );
        context.addParameterValues( 1, labelType, List.of( edge.labels.get( 0 ) ) );
        context.addParameterValues( 2, idType, List.of( edge.source ) );
        context.addParameterValues( 3, idType, List.of( edge.target ) );

        // execute all inserts
        drainInserts( enumerables.get( i ), edge.labels.size() );
        context.resetParameterValues();
    }


    /**
     * Modify operation, which inserts node properties.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param node the edge for which the properties are inserted
     */
    private static void nodePropertiesTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, AlgDataType valueType, PolyNode node ) {
        if ( !node.properties.isEmpty() ) {
            context.addParameterValues( 0, idType, Collections.nCopies( node.properties.size(), node.id ) );
            context.addParameterValues( 1, labelType, new ArrayList<>( node.properties.keySet() ) );
            context.addParameterValues( 2, valueType, new ArrayList<>( node.properties.values().stream().map( Object::toString ).collect( Collectors.toList() ) ) );
            drainInserts( enumerables.get( i ), node.properties.size() );
            context.resetParameterValues();
        }
    }


    /**
     * Modify operation, which inserts node id and labels.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param node the edge for which the properties are inserted
     */
    private static void nodeTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, PolyNode node ) {
        List<Object> labels = new ArrayList<>( node.labels );
        labels.add( 0, null ); // id + key (null ) is required for each node to enable label-less nodes
        context.addParameterValues( 0, idType, Collections.nCopies( labels.size(), node.id ) );
        context.addParameterValues( 1, labelType, labels );
        drainInserts( enumerables.get( i ), labels.size() );
        context.resetParameterValues();
    }


    /**
     * Modify operation, which deletes specific elements
     *
     * @param context dataContext of the delete operation
     * @param enumerables collection of all modify enumerables
     * @param order the order of modify operations
     * @return
     */
    private static Enumerable<?> sendDeletes( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order ) {
        int i = 0;
        Map<Long, Object> values = context.getParameterValues().get( 0 );
        Map<Long, AlgDataType> typeBackup = context.getParameterTypes();
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, 36 );
        AlgDataType labelType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );
        AlgDataType valueType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );

        context.resetParameterValues();
        for ( PolyType polyType : order ) {
            if ( polyType == PolyType.NODE ) {
                PolyNode node = (PolyNode) values.get( (long) i );

                // node table delete
                nodeTableDelete( context, enumerables, 2 * i, idType, labelType, node );

                // node property table delete
                nodePropertiesTableDelete( context, enumerables, 2 * i + 1, idType, labelType, node );

            } else if ( polyType == PolyType.EDGE ) {
                PolyEdge edge = (PolyEdge) values.get( (long) i );

                // edge table delete
                edgeTableDelete( context, enumerables, 2 * i, idType, labelType, edge );

                // edge property table delete
                edgePropertiesTableDelete( context, enumerables, 2 * i, idType, labelType, edge );

            }

            i++;
        }
        // restore original context
        context.resetParameterValues();
        context.setParameterValues( List.of( values ) );
        context.setParameterTypes( typeBackup );

        return Linq4j.singletonEnumerable( values.size() );

    }


    /**
     * Deletes properties for a given edge
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify enumerables
     * @param enumIndex index for the update operation
     * @param edge the edge, for which the properties are deleted
     */
    private static void edgePropertiesTableDelete( DataContext context, List<Function0<Enumerable<?>>> enumerables, int enumIndex, AlgDataType idType, AlgDataType labelType, PolyEdge edge ) {
        context.addParameterValues( 0, idType, List.of( edge.id ) );
        Enumerator<?> enumerableE = enumerables.get( enumIndex ).apply().enumerator();
        enumerableE.moveNext();
        context.resetParameterValues();
    }


    /**
     * Deletes label, id, source and target information for a given edge
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify enumerables
     * @param enumIndex index for the update operation
     * @param edge the edge, for which label, id, source and target information are deleted
     */
    private static void edgeTableDelete( DataContext context, List<Function0<Enumerable<?>>> enumerables, int enumIndex, AlgDataType idType, AlgDataType labelType, PolyEdge edge ) {
        context.addParameterValues( 0, idType, List.of( edge.id ) );
        context.addParameterValues( 1, labelType, List.of( edge.labels ) );
        Enumerator<?> enumerable = enumerables.get( enumIndex ).apply().enumerator();
        enumerable.moveNext();
        context.resetParameterValues();
    }


    /**
     * Deletes label, id, source and target information for a given edge
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify enumerables
     * @param enumIndex index for the update operation
     * @param node the edge, for which label, id, source and target information are deleted
     */
    private static void nodePropertiesTableDelete( DataContext context, List<Function0<Enumerable<?>>> enumerables, int enumIndex, AlgDataType idType, AlgDataType labelType, PolyNode node ) {
        context.addParameterValues( 0, idType, List.of( node.id ) );
        Enumerator<?> enumerable = enumerables.get( enumIndex ).apply().enumerator();
        enumerable.moveNext();
        context.resetParameterValues();
    }


    /**
     * Deletes label and id information for a given node
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify enumerables
     * @param enumIndex index for the update operation
     * @param node the node, for which label and id information are deleted
     */
    private static void nodeTableDelete( DataContext context, List<Function0<Enumerable<?>>> enumerables, int enumIndex, AlgDataType idType, AlgDataType labelType, PolyNode node ) {
        context.addParameterValues( 0, idType, List.of( node.id ) );
        Enumerator<?> enumerable = enumerables.get( enumIndex ).apply().enumerator();
        enumerable.moveNext();
        context.resetParameterValues();
    }


    /**
     * Executes a graph update operation.
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify operations
     * @param order the types included in the update operation
     * @return the result of the update operation
     */
    private static Enumerable<?> sendUpdates( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order ) {
        int i = 0;
        Map<Long, Object> values = context.getParameterValues().get( 0 );
        Map<Long, AlgDataType> typeBackup = context.getParameterTypes();
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, 36 );
        AlgDataType labelType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );
        AlgDataType valueType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );

        context.resetParameterValues();
        for ( PolyType polyType : order ) {
            if ( polyType == PolyType.NODE ) {
                PolyNode node = (PolyNode) values.get( (long) i );

                // delete previous nodes and properties
                nodeTableDelete( context, enumerables, 4 * i, idType, labelType, node );
                nodePropertiesTableDelete( context, enumerables, 4 * i + 1, idType, labelType, node );

                // insert new node and properties
                nodeTableInsert( context, enumerables, 4 * i + 2, idType, labelType, node );
                nodePropertiesTableInsert( context, enumerables, 4 * i + 3, idType, labelType, valueType, node );

            } else if ( polyType == PolyType.EDGE ) {
                PolyEdge edge = (PolyEdge) values.get( (long) i );

                // edge table delete
                edgeTableDelete( context, enumerables, 4 * i, idType, labelType, edge );
                edgePropertiesTableDelete( context, enumerables, 4 * i + 1, idType, labelType, edge );

                // edge table insert
                edgeTableInsert( context, enumerables, 4 * i + 2, idType, labelType, edge );
                edgePropertiesTableInsert( context, enumerables, 4 * i + 3, idType, labelType, valueType, edge );

            }

            i++;
        }
        // restore original context
        context.resetParameterValues();
        context.setParameterValues( List.of( values ) );
        context.setParameterTypes( typeBackup );

        return Linq4j.singletonEnumerable( values.size() );

    }


    /**
     * Executes all insert operations.
     *
     * @param activator functions, which execute the inserts
     * @param size the amount of inserts to execute
     */
    private static void drainInserts( Function0<Enumerable<?>> activator, int size ) {
        Enumerator<?> enumerable = activator.apply().enumerator();
        for ( int i1 = 0; i1 < size; i1++ ) {
            enumerable.moveNext();
        }
    }

}
