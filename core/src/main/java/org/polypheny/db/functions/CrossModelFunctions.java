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

package org.polypheny.db.functions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.GraphDeleteTargets;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;

public class CrossModelFunctions {

    // Existing normalized graph properties contain plain text. Keep this marker printable across SQL adapters.
    private static final String TYPED_GRAPH_VALUE_PREFIX = "~{PolyphenyGraphValue:v1}~:";

    private CrossModelFunctions() {
        // empty on purpose
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
    public static Enumerable<PolyValue[]> sendGraphModifies( DataContext context, List<Function0<Enumerable<?>>> enumerables,
            List<PolyType> order, Operation operation, boolean detachDelete ) {
        if ( operation == Modify.Operation.DELETE ) {
            return sendGraphDeletes( context, enumerables, order, detachDelete );
        }
        if ( operation == Modify.Operation.UPDATE ) {
            return sendGraphUpdates( context, enumerables, order );
        }
        return sendGraphInserts( context, enumerables, order );
    }


    private static Enumerable<PolyValue[]> sendGraphInserts( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order ) {
        Map<Long, PolyValue> values = context.getParameterValues().get( 0 );
        Map<Long, AlgDataType> typeBackup = context.getParameterTypes();
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE );
        AlgDataType labelType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );
        AlgDataType valueType = typeFactory.createPolyType( PolyType.VARCHAR, 255 );

        try {
            if ( order.contains( PolyType.EDGE ) ) {
                validateInsertEndpoints( context, enumerables.get( 2 * order.size() ), order, values, idType );
            }
            context.resetParameterValues();
            for ( int i = 0; i < order.size(); i++ ) {
                PolyValue element = values.get( (long) i );
                if ( element == null || element.isNull() ) {
                    continue;
                }
                if ( order.get( i ) == PolyType.NODE ) {
                    PolyNode node = element.asNode();
                    nodeTableInsert( context, enumerables, 2 * i, idType, labelType, node );
                    nodePropertiesTableInsert( context, enumerables, 2 * i + 1, idType, labelType, valueType, node );
                } else if ( order.get( i ) == PolyType.EDGE ) {
                    PolyEdge edge = element.asEdge();
                    edgeTableInsert( context, enumerables, 2 * i, idType, labelType, edge );
                    edgePropertiesTableInsert( context, enumerables, 2 * i + 1, idType, labelType, valueType, edge );
                }
            }
        } finally {
            context.resetParameterValues();
            context.setParameterValues( List.of( values ) );
            context.setParameterTypes( typeBackup );
        }

        return Linq4j.singletonEnumerable( new PolyValue[]{ PolyInteger.of( 1 ) } );
    }


    private static Enumerable<PolyValue[]> sendGraphUpdates( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order ) {
        List<Map<Long, PolyValue>> rows = context.getParameterValues();
        Map<Long, AlgDataType> types = context.getParameterTypes();
        long count = 0;
        try {
            validateUpdateTargets( context, enumerables, order, rows );
            for ( Map<Long, PolyValue> row : rows ) {
                if ( row.values().stream().allMatch( value -> value == null || value.isNull() ) ) {
                    continue;
                }
                sendUpdates( context, enumerables, order, row );
                count++;
            }
        } finally {
            context.resetParameterValues();
            context.setParameterValues( rows );
            context.setParameterTypes( types );
        }
        return Linq4j.singletonEnumerable( new PolyValue[]{ PolyLong.of( count ) } );
    }


    private static void validateInsertEndpoints( DataContext context, Function0<Enumerable<?>> nodeLookup, List<PolyType> order,
            Map<Long, PolyValue> values, AlgDataType idType ) {
        Set<PolyString> insertedNodes = new LinkedHashSet<>();
        Set<PolyString> endpoints = new LinkedHashSet<>();
        for ( int i = 0; i < order.size(); i++ ) {
            PolyValue element = values.get( (long) i );
            if ( element == null || element.isNull() ) {
                continue;
            }
            if ( order.get( i ) == PolyType.NODE ) {
                insertedNodes.add( element.asNode().id );
            } else if ( order.get( i ) == PolyType.EDGE ) {
                PolyEdge edge = element.asEdge();
                endpoints.add( edge.left );
                endpoints.add( edge.right );
            }
        }
        // Newly declared endpoints in this row will be inserted by the same CREATE.
        endpoints.removeAll( insertedNodes );
        requireGraphElements( context, nodeLookup, endpoints, idType, "Graph relationship requires exactly one source and one target node" );
    }


    private static void validateUpdateTargets( DataContext context, List<Function0<Enumerable<?>>> enumerables,
            List<PolyType> order, List<Map<Long, PolyValue>> rows ) {
        Set<PolyString> nodes = new LinkedHashSet<>();
        Set<PolyString> edges = new LinkedHashSet<>();
        Set<PolyString> endpoints = new LinkedHashSet<>();
        for ( Map<Long, PolyValue> row : rows ) {
            for ( int i = 0; i < order.size(); i++ ) {
                PolyValue element = row.get( (long) i );
                if ( element == null || element.isNull() ) {
                    continue;
                }
                PolyType expected = order.get( i );
                PolyType actual = element.getType();
                if ( (expected != PolyType.ANY && expected != actual) || (actual != PolyType.NODE && actual != PolyType.EDGE) ) {
                    throw new GenericRuntimeException( "Graph writes require a node or relationship binding, got %s.", actual );
                }
                if ( actual == PolyType.NODE ) {
                    nodes.add( element.asNode().id );
                } else {
                    PolyEdge edge = element.asEdge();
                    edges.add( edge.id );
                    endpoints.add( edge.left );
                    endpoints.add( edge.right );
                }
            }
        }

        AlgDataType idType = context.getStatement().getTransaction().getTypeFactory().createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE );
        int nodeIndex = order.stream().mapToInt( CrossModelFunctions::updateStatementCount ).sum();
        Function0<Enumerable<?>> nodeLookup = enumerables.get( nodeIndex );
        requireGraphElements( context, nodeLookup, nodes, idType, "Graph node disappeared before the update" );
        if ( !edges.isEmpty() ) {
            requireGraphElements( context, enumerables.get( nodeIndex + 1 ), edges, idType, "Graph relationship disappeared before the update" );
            endpoints.removeAll( nodes );
            requireGraphElements( context, nodeLookup, endpoints, idType, "Graph relationship requires exactly one source and one target node" );
        }
    }


    private static int updateStatementCount( PolyType type ) {
        return switch ( type ) {
            case NODE, EDGE -> 4;
            case ANY -> 8;
            default -> 0;
        };
    }


    private static void requireGraphElements( DataContext context, Function0<Enumerable<?>> lookup, Set<PolyString> ids, AlgDataType idType, String error ) {
        for ( PolyString id : ids ) {
            context.resetParameterValues();
            context.addParameterValues( 0, idType, List.of( id ) );
            try ( Enumerator<?> matches = lookup.apply().enumerator() ) {
                if ( !matches.moveNext() ) {
                    throw new GenericRuntimeException( error );
                }
            }
        }
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<?> tableToNodes( Enumerable<?> enumerable, PolyString label, List<PolyString> keys ) {
        return new AbstractEnumerable<PolyNode[]>() {
            @Override
            public Enumerator<PolyNode[]> enumerator() {
                if ( keys.size() > 1 ) {
                    return Linq4j.transform( enumerable.enumerator(), r -> {
                        PolyValue[] row = (PolyValue[]) r;
                        Map<PolyString, PolyValue> map = new HashMap<>();
                        for ( int i = 0; i < row.length; i++ ) {
                            map.put( keys.get( i ), row[i] );
                        }
                        return new PolyNode[]{ new PolyNode( new PolyDictionary( map ), PolyList.of( label ), PolyString.of( "n" ) ) };
                    } );
                }
                return Linq4j.transform(
                        enumerable.enumerator(),
                        r -> new PolyNode[]{
                                new PolyNode(
                                        new PolyDictionary( Map.of( keys.get( 0 ), PolyString.of( r.toString() ) ) ), PolyList.of( label ), PolyString.of( "n" ) ) } );
            }
        };
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<PolyNode[]> collectionToNodes( Enumerable<PolyValue[]> enumerable, PolyString label ) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyNode[]> enumerator() {
                return Linq4j.transform( enumerable.enumerator(), r -> {
                    PolyDocument doc = r[0].asDocument();
                    Map<PolyString, PolyValue> map = new HashMap<>();
                    for ( Entry<PolyString, PolyValue> entry : doc.entrySet() ) {
                        if ( entry.getKey().equals( PolyString.of( DocumentType.DOCUMENT_ID ) ) ) {
                            continue;
                        }
                        map.put( entry.getKey(), entry.getValue() );
                    }
                    return new PolyNode[]{ new PolyNode( doc.get( PolyString.of( DocumentType.DOCUMENT_ID ) ).asString(), new PolyDictionary( map ), PolyList.of( label ), PolyString.of( "n" ) ) };
                } );
            }
        }

                ;
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<PolyValue[]> nodesToCollection( Enumerable<PolyValue[]> enumerable, @Nullable PolyString label ) {
        PolyGraph graph = null;
        for ( PolyValue[] value : enumerable ) {
            graph = value[0].asGraph();
        }
        Function1<PolyMap<PolyString, PolyNode>, Stream<PolyNode>> node = elements -> elements.values().stream();
        Function1<PolyMap<PolyString, PolyEdge>, Stream<PolyEdge>> edge = elements -> elements.values().stream();
        if ( label != null ) {
            node = elements -> elements.values().stream().filter( n -> n.labels.contains( label ) );
            edge = elements -> elements.values().stream().filter( n -> n.labels.contains( label ) );
        }

        return Linq4j.asEnumerable( Stream.concat( node.apply( graph.getNodes() ), edge.apply( graph.getEdges() ) ).map( n -> {
            PolyDocument doc = new PolyDocument( n.properties.entrySet().stream().collect( Collectors.toMap( Entry::getKey, Entry::getValue ) ) );
            doc.put( PolyString.of( DocumentType.DOCUMENT_ID ), PolyString.of( n.id.value.substring( 0, 23 ) ) );
            return new PolyValue[]{ doc };
        } ).toList() );

    }


    /**
     * Modify operation, which inserts edge properties.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param edge the edge for which the properties are inserted
     */
    private static void edgePropertiesTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, AlgDataType valueType, PolyEdge edge ) {
        graphPropertiesTableInsert( context, enumerables.get( i ), idType, labelType, valueType, edge.id, edge.properties );
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
        context.addParameterValues( 2, idType, List.of( edge.left ) );
        context.addParameterValues( 3, idType, List.of( edge.right ) );

        // execute all inserts
        drainWrites( enumerables.get( i ), edge.labels.size() );
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
        graphPropertiesTableInsert( context, enumerables.get( i ), idType, labelType, valueType, node.id, node.properties );
    }


    private static void graphPropertiesTableInsert( DataContext context, Function0<Enumerable<?>> enumerable, AlgDataType idType,
            AlgDataType labelType, AlgDataType valueType, PolyString id, PolyDictionary properties ) {
        List<PolyString> keys = new ArrayList<>();
        List<PolyString> values = new ArrayList<>();
        for ( Entry<PolyString, PolyValue> entry : properties.entrySet() ) {
            PolyValue value = entry.getValue();
            if ( value != null && !value.isNull() ) {
                keys.add( entry.getKey() );
                values.add( PolyString.of( graphPropertyStorageValue( value ) ) );
            }
        }
        if ( !keys.isEmpty() ) {
            context.addParameterValues( 0, idType, Collections.nCopies( keys.size(), id ) );
            context.addParameterValues( 1, labelType, new ArrayList<>( keys ) );
            context.addParameterValues( 2, valueType, new ArrayList<>( values ) );
            drainWrites( enumerable, keys.size() );
            context.resetParameterValues();
        }
    }


    public static String graphPropertyStorageValue( PolyValue value ) {
        if ( value.isString() && !value.asString().value.startsWith( TYPED_GRAPH_VALUE_PREFIX ) ) {
            return value.asString().value;
        }
        return TYPED_GRAPH_VALUE_PREFIX + value.toTypedJson();
    }


    public static PolyValue readGraphPropertyStorageValue( PolyString stored ) {
        if ( stored.isNull() ) {
            return PolyNull.NULL;
        }
        if ( stored.value.startsWith( TYPED_GRAPH_VALUE_PREFIX ) ) {
            String json = stored.value.substring( TYPED_GRAPH_VALUE_PREFIX.length() );
            PolyValue decoded = PolyValue.fromTypedJson( json, PolyValue.class );
            if ( decoded != null && decoded.toTypedJson().equals( json ) ) {
                return decoded;
            }
        }
        return stored;
    }


    /**
     * Modify operation, which inserts node id and labels.
     *
     * @param enumerables collection of all modify enumerables
     * @param i index of modify operation
     * @param node the edge for which the properties are inserted
     */
    private static void nodeTableInsert( DataContext context, List<Function0<Enumerable<?>>> enumerables, int i, AlgDataType idType, AlgDataType labelType, PolyNode node ) {
        List<PolyValue> labels = new ArrayList<>( node.labels );
        labels.add( 0, PolyString.of( "$" ) ); // id + key (null ) is required for each node to enable label-less nodes
        context.addParameterValues( 0, idType, Collections.nCopies( labels.size(), node.id ) );
        context.addParameterValues( 1, labelType, labels );
        drainWrites( enumerables.get( i ), labels.size() );
        context.resetParameterValues();
    }


    private static Enumerable<PolyValue[]> sendGraphDeletes( DataContext context, List<Function0<Enumerable<?>>> inputs,
            List<PolyType> order, boolean detachDelete ) {
        List<Map<Long, PolyValue>> rows = context.getParameterValues();
        Map<Long, AlgDataType> types = context.getParameterTypes();
        GraphDeleteTargets targets = GraphDeleteTargets.collect( rows, order.size() );
        Set<PolyString> nodes = targets.nodes();
        Set<PolyString> edges = targets.edges();
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE );
        try {
            if ( !nodes.isEmpty() ) {
                context.resetParameterValues();
                try ( Enumerator<?> incidentEdges = inputs.get( 4 ).apply().enumerator() ) {
                    while ( incidentEdges.moveNext() ) {
                        Object[] edge = (Object[]) incidentEdges.current();
                        if ( !nodes.contains( edge[1] ) && !nodes.contains( edge[2] ) ) {
                            continue;
                        }
                        PolyString id = (PolyString) edge[0];
                        if ( !detachDelete && !edges.contains( id ) ) {
                            throw new GenericRuntimeException( "Cannot delete a node with relationships. Delete its relationships or use DETACH DELETE." );
                        }
                        edges.add( id );
                    }
                }
            }
            // The first two inputs delete relationships and their properties. The next two handle nodes.
            for ( PolyString id : edges ) {
                deleteGraphElement( context, inputs, 0, idType, id );
            }
            for ( PolyString id : nodes ) {
                deleteGraphElement( context, inputs, 2, idType, id );
            }
        } finally {
            context.resetParameterValues();
            context.setParameterValues( rows );
            context.setParameterTypes( types );
        }
        return Linq4j.singletonEnumerable( new PolyValue[]{ PolyLong.of( targets.rowCount() ) } );
    }


    private static void deleteGraphElement( DataContext context, List<Function0<Enumerable<?>>> inputs, int index, AlgDataType idType, PolyString id ) {
        context.resetParameterValues();
        context.addParameterValues( 0, idType, List.of( id ) );
        drainWrites( inputs.get( index ), 1 );
        drainWrites( inputs.get( index + 1 ), 1 );
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
        drainWrites( enumerables.get( enumIndex ), 1 );
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
        context.addParameterValues( 1, labelType, List.copyOf( edge.labels ) );
        drainWrites( enumerables.get( enumIndex ), 1 );
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
        drainWrites( enumerables.get( enumIndex ), 1 );
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
        drainWrites( enumerables.get( enumIndex ), 1 );
        context.resetParameterValues();
    }


    /**
     * Executes a graph update operation.
     *
     * @param context the dataContext for the update operation
     * @param enumerables collection of all modify operations
     * @param order the types included in the update operation
     */
    private static void sendUpdates( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order, Map<Long, PolyValue> values ) {
        JavaTypeFactory typeFactory = context.getStatement().getTransaction().getTypeFactory();
        AlgDataType idType = typeFactory.createPolyType( PolyType.VARCHAR, GraphType.ID_SIZE );
        AlgDataType labelType = typeFactory.createPolyType( PolyType.TEXT );
        AlgDataType valueType = typeFactory.createPolyType( PolyType.TEXT );

        context.resetParameterValues();
        int offset = 0;
        for ( int i = 0; i < order.size(); i++ ) {
            PolyType expected = order.get( i );
            int start = offset;
            offset += updateStatementCount( expected );
            PolyValue element = values.get( (long) i );
            if ( element == null || element.isNull() ) {
                continue;
            }
            if ( element instanceof PolyNode node ) {
                // delete previous nodes and properties
                nodeTableDelete( context, enumerables, start, idType, labelType, node );
                nodePropertiesTableDelete( context, enumerables, start + 1, idType, labelType, node );

                // insert new node and properties
                nodeTableInsert( context, enumerables, start + 2, idType, labelType, node );
                nodePropertiesTableInsert( context, enumerables, start + 3, idType, labelType, valueType, node );

            } else if ( element instanceof PolyEdge edge ) {
                int edgeStart = expected == PolyType.ANY ? start + updateStatementCount( PolyType.NODE ) : start;
                // edge table delete
                edgeTableDelete( context, enumerables, edgeStart, idType, labelType, edge );
                edgePropertiesTableDelete( context, enumerables, edgeStart + 1, idType, labelType, edge );

                // edge table insert
                edgeTableInsert( context, enumerables, edgeStart + 2, idType, labelType, edge );
                edgePropertiesTableInsert( context, enumerables, edgeStart + 3, idType, labelType, valueType, edge );
            }
        }
    }


    /**
     * Executes the bound write operations.
     *
     * @param activator the function that executes the writes
     * @param size the maximum number of rows the write returns
     */
    private static void drainWrites( Function0<Enumerable<?>> activator, int size ) {
        try ( Enumerator<?> enumerable = activator.apply().enumerator() ) {
            for ( int i = 0; i < size && enumerable.moveNext(); i++ ) {
                // The adapter performs the write while advancing its cursor.
            }
        }
    }


    @SuppressWarnings("unused")
    public static Enumerable<?> mergeNodeCollections( List<Enumerable<PolyNode>> enumerables ) {
        return Linq4j.concat( enumerables );
    }


    @SuppressWarnings("unused")
    public static Object docItem( String map, String key ) {
        Object value = PolyDictionary.fromString( map ).getOrDefault( key, null );
        return value != null ? value.toString() : null;
    }


    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> enumerableFromContext( DataContext context ) {
        return Linq4j.asEnumerable( context.getParameterValues().stream().map( e -> e.values().toArray( PolyValue[]::new ) ).toList() );
    }

}
