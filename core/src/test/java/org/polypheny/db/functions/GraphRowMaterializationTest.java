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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Linq4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class GraphRowMaterializationTest {

    @BeforeAll
    static void initializeTestHome() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void canonicalizesNestedValuesAndRetainsPathBindings() {
        PolyNode start = new PolyNode( PolyString.of( "start" ), new PolyDictionary(), List.of(), null );
        PolyNode end = new PolyNode( PolyString.of( "end" ), new PolyDictionary(), List.of(), null );
        PolyEdge match = relationship( start, end );
        PolyEdge update = relationship( start, end );
        PolyString relationships = PolyString.of( "relationships" );
        PolyPath path = PolyPath.create( List.of( Pair.of( null, start ), Pair.of( null, end ) ), List.of( Pair.of( null, match ) ) )
                .withBindings( Map.of( relationships, PolyList.of( match ) ) );
        PolyString field = PolyString.of( "value" );
        PolyDictionary projected = new PolyDictionary( Map.of( field, PolyList.of( match ) ) );
        PolyValue[] source = { PolyList.ofElements( PolyList.<PolyValue>ofElements( path, PolyNull.NULL ) ), PolyList.of( match ), projected, update };

        PolyValue[] result = Functions.materializeGraphRows( Linq4j.singletonEnumerable( source ) ).first();
        PolyEdge canonical = result[3].asEdge();
        canonical.properties.put( PolyString.of( "state" ), PolyString.of( "updated" ) );

        PolyPath rebound = result[0].asList().get( 0 ).asList().get( 0 ).asPath();
        assertEquals( path.id, rebound.id );
        assertSame( canonical, rebound.getEdges().get( 0 ) );
        assertSame( canonical, rebound.getBinding( relationships ).asList().get( 0 ) );
        assertSame( canonical, result[1].asList().get( 0 ) );
        assertSame( canonical, result[2].asMap().get( field ).asList().get( 0 ) );
        assertSame( PolyNull.NULL, result[0].asList().get( 0 ).asList().get( 1 ) );
    }


    private static PolyEdge relationship( PolyNode left, PolyNode right ) {
        return new PolyEdge( PolyString.of( "edge" ), new PolyDictionary(), List.of( PolyString.of( "R" ) ),
                left.id, right.id, EdgeDirection.LEFT_TO_RIGHT, null );
    }

}
