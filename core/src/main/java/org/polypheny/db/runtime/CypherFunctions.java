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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.serialize.PolySerializer;

@Deterministic
@Slf4j
public class CypherFunctions {

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
        for ( Object value : edge ) {
            Object[] o = (Object[]) value;
            edges.add( PolySerializer.deserializeAndCompress( ByteString.parseBase64( (String) o[1] ), PolyEdge.class ) );
        }

        return Linq4j.asEnumerable( edges );
    }


    public static Enumerable<PolyNode> toNode( Enumerable<?> node ) {
        List<PolyNode> nodes = new ArrayList<>();

        for ( Object value : node ) {
            Object[] o = (Object[]) value;
            nodes.add( PolySerializer.deserializeAndCompress( ByteString.parseBase64( (String) o[1] ), PolyNode.class ) );
        }

        return Linq4j.asEnumerable( nodes );

    }


    public static boolean propertiesMatch( PolyNode node, PolyDirectory properties ) {
        return node.matchesProperties( properties );
    }


    public static boolean labelsMatch( PolyNode node, List<String> labels ) {
        return node.matchesLabels( labels );
    }


    @SuppressWarnings("unused")
    public static GraphObject extractFrom( PolyPath path, int index ) {
        return path.get( index );
    }


}
