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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BsonUtil;

public class CrossModelFunctions {

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


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<?> tableToNodes( Enumerable<?> enumerable, String label, List<String> keys ) {
        return new AbstractEnumerable<PolyNode>() {
            @Override
            public Enumerator<PolyNode> enumerator() {
                if ( keys.size() > 1 ) {
                    return Linq4j.transform( enumerable.enumerator(), r -> {
                        Object[] row = (Object[]) r;
                        Map<String, Comparable<?>> map = new HashMap<>();
                        for ( int i = 0; i < row.length; i++ ) {
                            map.put( keys.get( i ), row[i].toString() );
                        }
                        return new PolyNode( new PolyDictionary( map ), List.of( label ), "n" );
                    } );
                }
                return Linq4j.transform( enumerable.enumerator(), r -> new PolyNode( new PolyDictionary( Map.of( keys.get( 0 ), r.toString() ) ), List.of( label ), "n" ) );
            }
        };
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<?> collectionToNodes( Enumerable<?> enumerable, String label ) {
        return new AbstractEnumerable<PolyNode>() {
            @Override
            public Enumerator<PolyNode> enumerator() {
                return Linq4j.transform( enumerable.enumerator(), r -> {
                    BsonDocument doc = BsonDocument.parse( (String) r );
                    Map<String, Comparable<?>> map = new HashMap<>();
                    for ( Entry<String, BsonValue> entry : doc.entrySet() ) {
                        if ( entry.getKey().equals( "_id" ) ) {
                            continue;
                        }
                        map.put( entry.getKey(), BsonUtil.getAsObject( entry.getValue() ) );
                    }
                    return new PolyNode( doc.get( "_id" ).asString().getValue(), new PolyDictionary( map ), List.of( label ), "n" );
                } );
            }
        };
    }


    @SuppressWarnings("UnusedDeclaration")
    public static Enumerable<?> nodesToCollection( Enumerable<PolyNode> enumerable ) {
        return new AbstractEnumerable<Object>() {
            @Override
            public Enumerator<Object> enumerator() {
                return Linq4j.transform(
                        enumerable.enumerator(),
                        r -> {
                            BsonDocument doc = new BsonDocument( r.properties.entrySet().stream().map( e -> new BsonElement( e.getKey(), new BsonString( e.getValue().toString() ) ) ).collect( Collectors.toList() ) );
                            doc.put( "_id", new BsonString( r.id.substring( 0, 23 ) ) );
                            return doc.toJson();
                        } );
            }
        };
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


    @SuppressWarnings("unused")
    public static Enumerable<?> mergeNodeCollections( List<Enumerable<PolyNode>> enumerables ) {
        return Linq4j.concat( enumerables );
    }

}
