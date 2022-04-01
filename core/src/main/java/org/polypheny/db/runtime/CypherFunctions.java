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
import java.util.Map.Entry;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Function0;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;
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

    public static void print( Object obj ) {
        System.out.println( obj.toString() );
    }


    @SuppressWarnings("unused")
    public static Enumerable<PolyPath> pathMatch( PolyGraph graph, PolyPath comp ) {
        return Linq4j.asEnumerable( graph.extract( comp ) );
    }


    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> nodeMatch( PolyGraph graph, PolyNode node ) {
        return Linq4j.asEnumerable( graph.extract( node ) );
    }


    public static Enumerable<PolyNode> nodeExtract( PolyGraph graph ) {
        return Linq4j.asEnumerable( graph.getNodes().values() );
    }


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

            if ( !id.equals( oldId ) ) {
                if ( oldId != null ) {
                    edges.add( new PolyEdge( oldId, new PolyDirectory( oldProps ), List.copyOf( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT ) );
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
            edges.add( new PolyEdge( oldId, new PolyDirectory( oldProps ), List.copyOf( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT ) );
        }

        return Linq4j.asEnumerable( edges );
    }


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

            if ( !id.equals( oldId ) ) {
                if ( oldId != null ) {
                    nodes.add( new PolyNode( oldId, new PolyDirectory( oldProps ), List.copyOf( oldLabels ) ) );
                }
                oldId = id;
                oldLabels = new HashSet<>();
                oldProps = new HashMap<>();
            }
            if ( label != null ) {
                // eventually no labels
                oldLabels.add( label );
            }
            if ( key != null ) {
                // eventually no properties present
                oldProps.put( key, val );
            }
        }

        if ( oldId != null ) {
            nodes.add( new PolyNode( oldId, new PolyDirectory( oldProps ), List.copyOf( oldLabels ) ) );
        }

        return Linq4j.asEnumerable( nodes );

    }


    @SuppressWarnings("unused")
    public static boolean hasProperty( PolyNode node, String property ) {
        return node.properties.containsKey( property );
    }


    @SuppressWarnings("unused")
    public static boolean hasLabel( PolyNode node, String label ) {
        return node.labels.contains( label );
    }


    @SuppressWarnings("unused")
    public static GraphObject extractFrom( PolyPath path, int index ) {
        return path.get( index );
    }


    @SuppressWarnings("unused")
    public static String extractProperty( GraphPropertyHolder holder, String key ) {
        if ( holder.getProperties().containsKey( key ) ) {
            return holder.getProperties().get( key ).toString();
        }
        return null;
    }


    @SuppressWarnings("unused")
    public static String extractLabel( GraphPropertyHolder holder ) {
        return holder.getLabels().get( 0 );
    }


    @SuppressWarnings("unused")
    public static List<String> extractLabels( GraphPropertyHolder holder ) {
        return holder.getLabels();
    }


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


    @SuppressWarnings("unused")
    public static PolyEdge adjustEdge( PolyEdge edge, PolyNode left, PolyNode right ) {
        // if isVariable is true, the node is only a placeholder and the id should not be replaced
        return edge.from( left.isVariable() ? null : left.id, right.isVariable() ? null : right.id );
    }


    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperty( GraphPropertyHolder target, String key, Object value ) {
        target.properties.put( key, value );
        return target;
    }


    @SuppressWarnings("unused")
    public static GraphPropertyHolder setLabels( GraphPropertyHolder target, List<String> labels, boolean replace ) {
        if ( replace ) {
            target.labels.clear();
        }
        target.setLabels( labels );
        return target;
    }


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


    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeLabels( GraphPropertyHolder target, List<String> labels ) {
        target.labels.removeAll( labels );
        return target;
    }


    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeProperty( GraphPropertyHolder target, String key ) {
        target.properties.remove( key );
        return target;
    }


    @SuppressWarnings("unused")
    public static Enumerable<?> sendGraphModifies( DataContext context, List<Function0<Enumerable<?>>> enumerables, List<PolyType> order ) {
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

                // node table
                context.addParameterValues( 0, idType, Collections.nCopies( node.labels.size(), node.id ) );
                context.addParameterValues( 1, labelType, new ArrayList<>( node.labels ) );
                drainInserts( enumerables.get( 2 * i ), node.labels.size() );
                context.resetParameterValues();

                // node property table
                if ( !node.properties.isEmpty() ) {
                    context.addParameterValues( 0, idType, Collections.nCopies( node.properties.size(), node.id ) );
                    for ( Entry<String, Object> entry : node.properties.entrySet() ) {
                        context.addParameterValues( 1, labelType, List.of( entry.getKey() ) );
                        context.addParameterValues( 2, valueType, List.of( entry.getValue() ) );
                    }
                    drainInserts( enumerables.get( 2 * i + 1 ), node.properties.size() );
                    context.resetParameterValues();
                }

            } else if ( polyType == PolyType.EDGE ) {
                PolyEdge edge = (PolyEdge) values.get( (long) i );

                // edge table
                context.addParameterValues( 0, idType, Collections.nCopies( edge.labels.size(), edge.id ) );
                context.addParameterValues( 1, labelType, List.of( edge.labels ) );
                context.addParameterValues( 2, idType, List.of( edge.source ) );
                context.addParameterValues( 3, idType, List.of( edge.target ) );
                drainInserts( enumerables.get( 2 * i ), edge.labels.size() );
                context.resetParameterValues();

                // edge property table
                if ( !edge.properties.isEmpty() ) {
                    context.addParameterValues( 0, idType, Collections.nCopies( edge.properties.size(), edge.id ) );
                    for ( Entry<String, Object> entry : edge.properties.entrySet() ) {
                        context.addParameterValues( 1, labelType, List.of( entry.getKey() ) );
                        context.addParameterValues( 2, valueType, List.of( entry.getValue() ) );
                    }

                    drainInserts( enumerables.get( 2 * i + 1 ), edge.labels.size() );
                    context.resetParameterValues();
                }

            }

            i++;
        }
        // restore original context
        context.resetParameterValues();
        context.setParameterValues( List.of( values ) );
        context.setParameterTypes( typeBackup );

        return Linq4j.singletonEnumerable( 1 );
    }


    private static void drainInserts( Function0<Enumerable<?>> activator, int size ) {
        Enumerator<?> enumerable;
        enumerable = activator.apply().enumerator();
        for ( int i1 = 0; i1 < size; i1++ ) {
            enumerable.moveNext();
        }
    }

}
