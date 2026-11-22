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

package org.polypheny.db.prisminterface.utils;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyInterval;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.ProtoBigDecimal;
import org.polypheny.prism.ProtoBinary;
import org.polypheny.prism.ProtoBoolean;
import org.polypheny.prism.ProtoDate;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoDouble;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoFloat;
import org.polypheny.prism.ProtoGraph;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoInterval;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoLong;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoNull;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoTime;
import org.polypheny.prism.ProtoTimestamp;
import org.polypheny.prism.ProtoValue;

public class PolyValueSerializer {


    private final boolean supportsGraphValues;


    PolyValueSerializer( boolean supportsGraphValues ) {
        this.supportsGraphValues = supportsGraphValues;
    }


    List<ProtoValue> serializeList( List<PolyValue> valuesList ) {
        return valuesList.stream().map( this::serializeValue ).toList();
    }


    public static ProtoValue serialize( PolyValue polyValue ) {
        return new PolyValueSerializer( true ).serializeValue( polyValue );
    }


    private ProtoValue serializeValue( PolyValue polyValue ) {
        if ( polyValue == null || polyValue.isNull() ) {
            return serializeAsProtoNull();
        }
        return switch ( polyValue.getType() ) {
            case BOOLEAN -> serializeAsProtoBoolean( polyValue.asBoolean() );
            case TINYINT, SMALLINT, INTEGER -> serializeAsProtoInteger( polyValue.asInteger() );
            case BIGINT -> serializeAsProtoLong( polyValue.asLong() );
            case DECIMAL -> serializeAsProtoBigDecimal( polyValue.asBigDecimal() );
            case REAL, FLOAT -> serializeAsProtoFloat( polyValue.asFloat() );
            case DOUBLE -> serializeAsProtoDouble( polyValue.asDouble() );
            case DATE -> serializeAsProtoDate( polyValue.asDate() );
            case TIME -> serializeAsProtoTime( polyValue.asTime() );
            case TIMESTAMP -> serializeAsProtoTimestamp( polyValue.asTimestamp() );
            case INTERVAL -> serializeAsProtoInterval( polyValue.asInterval() );
            case CYPHER_TEMPORAL -> serializeAsProtoString( PolyString.of( polyValue.toString() ) );
            case CHAR, VARCHAR, TEXT -> serializeAsProtoString( polyValue.asString() );
            case BINARY, VARBINARY -> serializeAsProtoBinary( polyValue.asBinary() );
            case NULL -> serializeAsProtoNull();
            case ARRAY -> serializeAsProtoList( polyValue.asList() );
            case DOCUMENT, MAP -> ProtoValue.newBuilder().setDocument( buildProtoDocument( polyValue.asMap() ) ).build();
            case GEOMETRY -> serializeGeometry( polyValue.asGeometry() );
            case IMAGE, VIDEO, AUDIO, FILE -> serializeAsProtoFile( polyValue.asBlob() ); // used
            case NODE, EDGE, PATH, GRAPH -> serializeGraphValue( polyValue );
            case DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, SYMBOL, JSON, MULTISET, USER_DEFINED_TYPE, ANY -> throw new NotImplementedException( "Serialization of " + polyValue.getType() + " to proto not implemented" );
        };
    }


    private static ProtoValue serializeGeometry( PolyGeometry geometry ) {
        ProtoString asString = ProtoString.newBuilder().setString( geometry.toString() ).build(); // todo add own value, taken from @danylokravchenko
        return ProtoValue.newBuilder().setString( asString ).build();
    }


    ProtoDocument buildProtoDocument( Map<? extends PolyValue, ? extends PolyValue> properties ) {
        // Prism represents maps with string keys as a document value.
        ProtoDocument.Builder document = ProtoDocument.newBuilder();
        properties.forEach( ( key, value ) -> {
            if ( !(key instanceof PolyString name) || name.isNull() ) {
                throw new NotImplementedException( "Prism requires non-null string map keys." );
            }
            document.putEntries( name.getValue(), serializeValue( value ) );
        } );
        return document.build();
    }


    private ProtoValue serializeAsProtoList( PolyList<PolyValue> polyList ) {
        return ProtoValue.newBuilder()
                .setList( serializeToProtoList( polyList ) )
                .build();

    }


    private ProtoList serializeToProtoList( PolyList<PolyValue> polyList ) {
        return ProtoList.newBuilder()
                .addAllValues( serializeList( polyList.getValue() ) )
                .build();
    }


    private static ProtoValue serializeAsProtoFile( PolyBlob polyBlob ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBlob.getValue() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    private static ProtoValue serializeAsProtoInterval( PolyInterval polyInterval ) {
        return ProtoValue.newBuilder()
                .setInterval( ProtoInterval.newBuilder().setMonths( polyInterval.getMonths() ).setMilliseconds( polyInterval.getMillis() ).build() )
                .build();
    }


    public static ProtoValue serializeAsProtoBoolean( PolyBoolean polyBoolean ) {
        if ( polyBoolean.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }


    public static ProtoValue serializeAsProtoInteger( PolyInteger polyInteger ) {
        if ( polyInteger.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .build();
    }


    public static ProtoValue serializeAsProtoLong( PolyLong polyLong ) {
        if ( polyLong.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .build();
    }


    public static ProtoValue serializeAsProtoBinary( PolyBinary polyBinary ) {
        if ( polyBinary.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.getValue() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    public static ProtoValue serializeAsProtoDate( PolyDate polyDate ) {
        if ( polyDate.millisSinceEpoch == null ) {
            return serializeAsProtoNull();
        }
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getDaysSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .build();
    }


    public static ProtoValue serializeAsProtoDouble( PolyDouble polyDouble ) {
        if ( polyDouble.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.doubleValue() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .build();
    }


    public static ProtoValue serializeAsProtoFloat( PolyFloat polyFloat ) {
        if ( polyFloat.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.floatValue() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .build();
    }


    public static ProtoValue serializeAsProtoString( PolyString polyString ) {
        if ( polyString.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.getValue() )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .build();
    }


    public static ProtoValue serializeAsProtoTime( PolyTime polyTime ) {
        if ( polyTime.ofDay == null ) {
            return serializeAsProtoNull();
        }
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setTime( polyTime.getOfDay() )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .build();
    }


    public static ProtoValue serializeAsProtoTimestamp( PolyTimestamp polyTimestamp ) {
        if ( polyTimestamp.millisSinceEpoch == null ) {
            return serializeAsProtoNull();
        }
        ProtoTimestamp protoTimestamp = ProtoTimestamp.newBuilder()
                .setTimestamp( polyTimestamp.getMillisSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setTimestamp( protoTimestamp )
                .build();
    }


    private static ProtoValue serializeAsProtoNull() {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }


    public static ProtoValue serializeAsProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        if ( polyBigDecimal.value == null ) {
            return serializeAsProtoNull();
        }
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( polyBigDecimal.getValue().unscaledValue().toByteArray() ) )
                .setScale( polyBigDecimal.getValue().scale() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .build();
    }

    private ProtoValue serializeGraphValue( PolyValue value ) {
        if ( !supportsGraphValues ) {
            throw new PIServiceException( "Graph values in cells require Prism 2.3 or later.", "0A000" );
        }
        return switch ( value.getType() ) {
            case NODE -> ProtoValue.newBuilder().setNode( buildProtoNode( value.asNode() ) ).build();
            case EDGE -> ProtoValue.newBuilder().setEdge( buildProtoEdge( value.asEdge() ) ).build();
            case PATH -> ProtoValue.newBuilder().setPath( buildProtoPath( value.asPath() ) ).build();
            case GRAPH -> ProtoValue.newBuilder().setGraph( buildProtoGraph( value.asGraph() ) ).build();
            default -> throw new IllegalArgumentException( "Expected a graph value, got " + value.getType() );
        };
    }


    private ProtoPath buildProtoPath( PolyPath path ) {
        return ProtoPath.newBuilder()
                .addAllNodes( path.getNodes().stream().map( this::buildProtoNode ).toList() )
                .addAllEdges( path.getEdges().stream().map( this::buildProtoEdge ).toList() )
                .build();
    }


    private ProtoGraph buildProtoGraph( PolyGraph graph ) {
        return ProtoGraph.newBuilder()
                .addAllNodes( graph.getNodes().values().stream().map( this::buildProtoNode ).toList() )
                .addAllEdges( graph.getEdges().values().stream().map( this::buildProtoEdge ).toList() )
                .build();
    }


    void addGraphElements( GraphFrame.Builder frame, PolyValue value ) {
        // Prism has no null GraphElement. An optional match can yield no element.
        if ( value == null || value.isNull() ) {
            return;
        }
        switch ( value.getType() ) {
            case NODE -> frame.addElement( GraphElement.newBuilder().setNode( buildProtoNode( value.asNode() ) ) );
            case EDGE -> frame.addElement( GraphElement.newBuilder().setEdge( buildProtoEdge( value.asEdge() ) ) );
            case PATH -> value.asPath().getPath().forEach( element -> addGraphElements( frame, element ) );
            case GRAPH -> {
                value.asGraph().getNodes().values().forEach( node -> addGraphElements( frame, node ) );
                value.asGraph().getEdges().values().forEach( edge -> addGraphElements( frame, edge ) );
            }
            default -> throw new NotImplementedException( "Prism cannot encode a " + value.getType() + " value in a graph frame" );
        }
    }


    private ProtoNode buildProtoNode( PolyNode node ) {
        ProtoNode.Builder result = ProtoNode.newBuilder()
                .setId( node.getId().getValue() )
                .addAllLabels( node.getLabels().stream().map( PolyString::getValue ).toList() );
        node.properties.forEach( ( key, value ) -> result.putProperties( key.getValue(), serializeValue( value ) ) );
        String name = getVariableName( node );
        if ( name != null ) {
            result.setName( name );
        }
        return result.build();
    }


    private ProtoEdge buildProtoEdge( PolyEdge edge ) {
        ProtoEdge.Builder result = ProtoEdge.newBuilder()
                .setId( edge.getId().getValue() )
                .addAllLabels( edge.getLabels().stream().map( PolyString::getValue ).toList() )
                .setLeft( edge.getLeft().getValue() )
                .setRight( edge.getRight().getValue() )
                .setDirection( switch ( edge.getDirection() ) {
                    case LEFT_TO_RIGHT -> ProtoEdge.Direction.LEFT_TO_RIGHT;
                    case RIGHT_TO_LEFT -> ProtoEdge.Direction.RIGHT_TO_LEFT;
                    case NONE -> ProtoEdge.Direction.NONE;
                } );
        edge.properties.forEach( ( key, value ) -> result.putProperties( key.getValue(), serializeValue( value ) ) );
        String name = getVariableName( edge );
        if ( name != null ) {
            result.setName( name );
        }
        return result.build();
    }


    private static String getVariableName( GraphPropertyHolder element ) {
        PolyString variableName = element.getVariableName();
        return variableName == null ? null : variableName.getValue();
    }

}
