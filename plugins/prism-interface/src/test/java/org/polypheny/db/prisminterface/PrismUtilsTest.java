/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.ProtoValue.ValueCase;
import org.polypheny.prism.Row;

public class PrismUtilsTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    @Test
    public void serializeToRowsSimpleTest() {
        List<PolyValue> row1 = List.of( new PolyString( "test" ), new PolyInteger( 42 ), new PolyBoolean( false ) );
        List<PolyValue> row2 = List.of( new PolyString( "example" ), new PolyInteger( 24 ), new PolyBoolean( true ) );

        List<List<PolyValue>> inputRows = List.of( row1, row2 );
        List<Row> prismRows = PrismUtils.serializeToRows( inputRows );

        Row prismRow1 = prismRows.get( 0 );
        List<ProtoValue> outputRow1 = prismRow1.getValuesList();
        assertEquals( ValueCase.STRING, outputRow1.get( 0 ).getValueCase() );
        assertEquals( ValueCase.INTEGER, outputRow1.get( 1 ).getValueCase() );
        assertEquals( ValueCase.BOOLEAN, outputRow1.get( 2 ).getValueCase() );
        assertEquals( "test", outputRow1.get( 0 ).getString().getString() );
        assertEquals( 42, outputRow1.get( 1 ).getInteger().getInteger() );
        assertFalse( outputRow1.get( 2 ).getBoolean().getBoolean() );

        Row prismRow2 = prismRows.get( 1 );
        List<ProtoValue> outputRow2 = prismRow2.getValuesList();
        assertEquals( ValueCase.STRING, outputRow2.get( 0 ).getValueCase() );
        assertEquals( ValueCase.INTEGER, outputRow2.get( 1 ).getValueCase() );
        assertEquals( ValueCase.BOOLEAN, outputRow2.get( 2 ).getValueCase() );
        assertEquals( "example", outputRow2.get( 0 ).getString().getString() );
        assertEquals( 24, outputRow2.get( 1 ).getInteger().getInteger() );
        assertTrue( outputRow2.get( 2 ).getBoolean().getBoolean() );
    }


    @Test
    public void serializeToRowsEmptyRowTest() {
        List<List<PolyValue>> inputRows = List.of();
        List<Row> prismRows = PrismUtils.serializeToRows( inputRows );
        assertTrue( prismRows.isEmpty() );
    }


    @Test
    public void buildGraphFrameWithNodesTest() {
        PolyDictionary properties1 = PolyDictionary.ofDict( Map.of( new PolyString( "key1" ), new PolyString( "value1" ) ) );
        PolyDictionary properties2 = PolyDictionary.ofDict( Map.of( new PolyString( "key2" ), new PolyString( "value2" ) ) );

        PolyNode node1 = new PolyNode( properties1, List.of( new PolyString( "label1" ) ), new PolyString( "node1" ) );
        PolyNode node2 = new PolyNode( properties2, List.of( new PolyString( "label2" ) ), new PolyString( "node2" ) );

        List<List<PolyValue>> data = List.of(
                List.of( node1 ),
                List.of( node2 )
        );

        Frame result = PrismUtils.buildGraphFrame( true, data );

        assertTrue( result.getIsLast() );
        assertEquals( 2, result.getGraphFrame().getNodesCount() );
        assertEquals( "node1", result.getGraphFrame().getNodes( 0 ).getName() );
        assertEquals( "node2", result.getGraphFrame().getNodes( 1 ).getName() );
    }


    @Test
    public void buildGraphFrameWithEmptyNodesTest() {
        List<List<PolyValue>> data = List.of();

        Frame result = PrismUtils.buildGraphFrame( true, data );

        assertTrue( result.getIsLast() );
        assertEquals( 0, result.getGraphFrame().getNodesCount() );
    }


    @Test
    public void buildGraphFrameWithEdgesTest() {
        PolyDictionary properties1 = PolyDictionary.ofDict( Map.of( new PolyString( "key1" ), new PolyString( "value1" ) ) );
        PolyDictionary properties2 = PolyDictionary.ofDict( Map.of( new PolyString( "key2" ), new PolyString( "value2" ) ) );

        PolyEdge node1 = new PolyEdge( properties1, List.of( new PolyString( "label1" ) ), new PolyString( "node4" ), new PolyString( "node5" ), EdgeDirection.NONE, new PolyString( "edge1" ) );
        PolyEdge node2 = new PolyEdge( properties2, List.of( new PolyString( "label2" ) ), new PolyString( "node4" ), new PolyString( "node5" ), EdgeDirection.NONE, new PolyString( "edge2" ) );

        List<List<PolyValue>> data = List.of(
                List.of( node1 ),
                List.of( node2 )
        );

        Frame result = PrismUtils.buildGraphFrame( false, data );

        assertFalse( result.getIsLast() );
        assertEquals( 2, result.getGraphFrame().getEdgesCount() );
        assertEquals( "edge1", result.getGraphFrame().getEdges( 0 ).getName() );
        assertEquals( "edge2", result.getGraphFrame().getEdges( 1 ).getName() );
    }

}
