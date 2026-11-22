/*
 * Copyright 2019-2026 The Polypheny Project
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.Pair;


/** Runs a whole-pattern MERGE for each incoming Cypher row, in input order. */
public final class CypherMergeRows {

    private CypherMergeRows() {
    }


    public record Node( int binding, boolean bound, int properties, List<PolyString> labels ) {

        public Node {
            labels = List.copyOf( labels );
        }
    }


    public record Relationship( int binding, int properties, PolyString type, EdgeDirection direction ) {
    }


    public record Pattern( List<Node> nodes, List<Relationship> relationships, int pathBinding, int inputCount, List<PolyString> outputNames ) {

        public Pattern {
            nodes = List.copyOf( nodes );
            relationships = List.copyOf( relationships );
            outputNames = List.copyOf( outputNames );
        }


        PolyString bindingName( int slot ) {
            return slot < 0 ? null : outputNames.get( slot );
        }


        public List<PolyType> insertionTypes() {
            List<PolyType> types = new ArrayList<>();
            Set<Integer> seen = new HashSet<>();
            for ( Node node : nodes ) {
                if ( !node.bound && (node.binding < 0 || seen.add( node.binding )) ) {
                    types.add( PolyType.NODE );
                }
            }
            for ( Relationship relationship : relationships ) {
                types.add( PolyType.EDGE );
            }
            return List.copyOf( types );
        }
    }


    public static Enumerable<PolyValue[]> execute( DataContext context, Enumerable<PolyValue[]> input,
            Function0<Enumerable<PolyValue[]>> scan, Pattern pattern, Function0<Enumerable<PolyValue[]>> insert,
            Function0<Enumerable<PolyValue[]>> update, List<PolyType> updateTypes,
            Function1<PolyValue[], PolyValue[]> properties, Function1<PolyValue[], PolyValue[]> onCreate,
            Function1<PolyValue[], PolyValue[]> onMatch, boolean countOnly ) {
        // Capture the upstream rows (and finish their writes) before opening the graph scan.
        List<PolyValue[]> source = new ArrayList<>();
        try ( Enumerator<PolyValue[]> upstream = input.enumerator() ) {
            while ( upstream.moveNext() ) {
                source.add( upstream.current().clone() );
            }
        }
        List<PolyValue[]> result = new ArrayList<>();
        if ( source.isEmpty() ) {
            return Linq4j.asEnumerable( result );
        }

        PolyGraph graph;
        try ( Enumerator<PolyValue[]> graphs = scan.apply().enumerator() ) {
            graph = graphs.moveNext() ? graphs.current()[0].asGraph() : new PolyGraph( PolyMap.of( Map.of() ), PolyMap.of( Map.of() ) );
        }
        List<PolyType> insertTypes = pattern.insertionTypes();
        for ( PolyValue[] row : source ) {
            PolyValue[] bindings = new PolyValue[pattern.outputNames.size()];
            for ( int i = 0; i < pattern.inputCount; i++ ) {
                bindings[i] = currentValue( graph, row[i] );
            }
            PolyPath template = template( pattern, properties.apply( bindings ) );
            // Resolve all matches before applying the branch's SET operations.
            // Changes can affect the next input row, but cannot change this row's set of matches.
            List<PolyPath> matches = new ArrayList<>();
            for ( PolyPath path : graph.matchPaths( template ) ) {
                if ( matchesBindings( pattern, path, bindings ) ) {
                    matches.add( path );
                }
            }
            if ( matches.isEmpty() ) {
                List<GraphPropertyHolder> created = new ArrayList<>();
                PolyPath path = create( pattern, template, bindings, created );
                bindPattern( pattern, path, bindings );
                PolyValue[] updates = onCreate.apply( bindings );
                write( context, insert, insertTypes, created.toArray( PolyValue[]::new ) );

                Set<PolyString> inserted = new HashSet<>();
                created.forEach( element -> inserted.add( element.id ) );
                update( context, update, updateTypes, updates, inserted );
                for ( GraphPropertyHolder element : created ) {
                    if ( element instanceof PolyNode node ) {
                        graph.getNodes().put( node.id, node );
                    } else {
                        graph.getEdges().put( element.id, element.asEdge() );
                    }
                }
                result.add( bindings );
            } else {
                for ( PolyPath path : matches ) {
                    PolyValue[] matched = bindings.clone();
                    bindPattern( pattern, path.rebindGraphValues( value -> currentValue( graph, value ) ), matched );
                    PolyValue[] updates = onMatch.apply( matched );
                    boolean changed = update( context, update, updateTypes, updates, Set.of() );
                    if ( changed || !countOnly ) {
                        result.add( matched );
                    }
                }
            }
        }
        if ( !countOnly ) {
            result.forEach( row -> exposeBindings( pattern, row ) );
        }
        return Linq4j.asEnumerable( result );
    }


    private static PolyValue currentValue( PolyGraph graph, PolyValue value ) {
        if ( value instanceof PolyNode node ) {
            return graph.getNodes().getOrDefault( node.id, node );
        }
        if ( value instanceof PolyEdge edge ) {
            return graph.getEdges().getOrDefault( edge.id, edge );
        }
        if ( value instanceof PolyPath path ) {
            return path.rebindGraphValues( part -> currentValue( graph, part ) );
        }
        if ( value instanceof PolyList<?> list ) {
            return PolyList.copyOf( list.stream().map( element -> currentValue( graph, element ) ).toList() );
        }
        if ( value instanceof PolyDictionary dictionary ) {
            Map<PolyString, PolyValue> entries = new HashMap<>();
            dictionary.forEach( ( key, element ) -> entries.put( key, currentValue( graph, element ) ) );
            return new PolyDictionary( entries );
        }
        return value;
    }


    private static PolyPath template( Pattern pattern, PolyValue[] row ) {
        List<Pair<PolyString, PolyNode>> nodes = new ArrayList<>();
        for ( Node node : pattern.nodes ) {
            PolyString name = pattern.bindingName( node.binding );
            nodes.add( Pair.of( name, new PolyNode( properties( row, node.properties ), node.labels, name ) ) );
        }
        List<Pair<PolyString, PolyEdge>> edges = new ArrayList<>();
        for ( int i = 0; i < pattern.relationships.size(); i++ ) {
            Relationship relationship = pattern.relationships.get( i );
            PolyString name = pattern.bindingName( relationship.binding );
            edges.add( Pair.of( name, new PolyEdge( properties( row, relationship.properties ), List.of( relationship.type ),
                    nodes.get( i ).right.id, nodes.get( i + 1 ).right.id, relationship.direction, name ) ) );
        }
        return PolyPath.create( nodes, edges );
    }


    private static PolyDictionary properties( PolyValue[] row, int index ) {
        if ( index < 0 ) {
            return new PolyDictionary();
        }
        PolyDictionary properties = (PolyDictionary) row[index];
        for ( PolyValue value : properties.values() ) {
            if ( value == null || value.isNull() ) {
                throw new GenericRuntimeException( "MergeReadOwnWrites: MERGE pattern properties cannot be null." );
            }
            CypherFunctions.validatePropertyValue( value );
        }
        return new PolyDictionary( properties );
    }


    private static boolean matchesBindings( Pattern pattern, PolyPath path, PolyValue[] bindings ) {
        for ( int i = 0; i < pattern.nodes.size(); i++ ) {
            Node node = pattern.nodes.get( i );
            if ( node.bound && !path.getNodes().get( i ).id.equals( boundNode( bindings[node.binding] ).id ) ) {
                return false;
            }
        }
        return true;
    }


    private static PolyNode boundNode( PolyValue value ) {
        if ( value instanceof PolyNode node ) {
            return node;
        }
        throw new GenericRuntimeException( "MERGE requires a non-null node binding for a relationship endpoint." );
    }


    private static PolyPath create( Pattern pattern, PolyPath template, PolyValue[] bindings, List<GraphPropertyHolder> created ) {
        List<Pair<PolyString, PolyNode>> nodes = new ArrayList<>();
        for ( int i = 0; i < pattern.nodes.size(); i++ ) {
            Node part = pattern.nodes.get( i );
            PolyNode node = template.getNodes().get( i );
            if ( part.bound || part.binding >= 0 && bindings[part.binding] != null ) {
                node = boundNode( bindings[part.binding] );
            } else {
                created.add( node );
                if ( part.binding >= 0 ) {
                    bindings[part.binding] = node;
                }
            }
            nodes.add( Pair.of( pattern.bindingName( part.binding ), node ) );
        }
        List<Pair<PolyString, PolyEdge>> relationships = new ArrayList<>();
        for ( int i = 0; i < pattern.relationships.size(); i++ ) {
            PolyEdge edge = template.getEdges().get( i );
            PolyString left = nodes.get( i ).right.id;
            PolyString right = nodes.get( i + 1 ).right.id;
            if ( edge.direction == EdgeDirection.RIGHT_TO_LEFT ) {
                PolyString reverse = left;
                left = right;
                right = reverse;
            }
            PolyEdge createdEdge = new PolyEdge( edge.properties, edge.labels, left, right, EdgeDirection.LEFT_TO_RIGHT, edge.getVariableName() );
            created.add( createdEdge );
            relationships.add( Pair.of( pattern.bindingName( pattern.relationships.get( i ).binding ), createdEdge ) );
        }
        return PolyPath.create( nodes, relationships );
    }


    private static void bindPattern( Pattern pattern, PolyPath path, PolyValue[] row ) {
        for ( int i = 0; i < pattern.nodes.size(); i++ ) {
            Node node = pattern.nodes.get( i );
            if ( node.binding >= 0 && !node.bound ) {
                row[node.binding] = path.getNodes().get( i );
            }
        }
        for ( int i = 0; i < pattern.relationships.size(); i++ ) {
            int slot = pattern.relationships.get( i ).binding;
            if ( slot >= 0 ) {
                row[slot] = path.getEdges().get( i );
            }
        }
        if ( pattern.pathBinding >= 0 ) {
            row[pattern.pathBinding] = path;
        }
    }


    private static void exposeBindings( Pattern pattern, PolyValue[] row ) {
        // Both branches use canonical graph objects during MERGE. Attach the output names only
        // after all writes, so copies cannot obscure later changes to a shared binding's labels.
        for ( int i = 0; i < row.length; i++ ) {
            if ( row[i] instanceof PolyNode node ) {
                row[i] = node.copyNamed( pattern.bindingName( i ) );
            } else if ( row[i] instanceof PolyEdge edge ) {
                row[i] = edge.copyNamed( pattern.bindingName( i ) );
            }
        }
        if ( pattern.pathBinding >= 0 ) {
            PolyPath path = row[pattern.pathBinding].asPath();
            List<Pair<PolyString, PolyNode>> nodes = new ArrayList<>();
            List<Pair<PolyString, PolyEdge>> edges = new ArrayList<>();
            for ( int i = 0; i < pattern.nodes.size(); i++ ) {
                PolyString name = pattern.bindingName( pattern.nodes.get( i ).binding );
                nodes.add( Pair.of( name, path.getNodes().get( i ).copyNamed( name ) ) );
            }
            for ( int i = 0; i < pattern.relationships.size(); i++ ) {
                PolyString name = pattern.bindingName( pattern.relationships.get( i ).binding );
                edges.add( Pair.of( name, path.getEdges().get( i ).copyNamed( name ) ) );
            }
            row[pattern.pathBinding] = PolyPath.create( nodes, edges );
        }
    }


    private static boolean update( DataContext context, Function0<Enumerable<PolyValue[]>> collector,
            List<PolyType> types, PolyValue[] values, Set<PolyString> inserted ) {
        PolyValue[] parameters = new PolyValue[types.size()];
        Set<PolyString> written = new HashSet<>( inserted );
        boolean hasWrites = false;
        for ( int i = 0; i < types.size(); i++ ) {
            PolyValue value = values[i];
            if ( value instanceof GraphPropertyHolder element && written.add( element.id ) ) {
                parameters[i] = value;
                hasWrites = true;
            }
        }
        if ( hasWrites ) {
            write( context, collector, types, parameters );
        }
        return hasWrites;
    }


    private static void write( DataContext context, Function0<Enumerable<PolyValue[]>> collector, List<PolyType> types, PolyValue[] row ) {
        Functions.streamGraphRows( context, Linq4j.asEnumerable( List.<PolyValue[]>of( row ) ), collector, types, false );
    }

}
