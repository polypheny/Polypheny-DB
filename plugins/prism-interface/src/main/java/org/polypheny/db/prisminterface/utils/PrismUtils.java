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

package org.polypheny.db.prisminterface.utils;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.prisminterface.statements.PIPreparedStatement;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.streaming.StreamingIndex;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.DocumentFrame;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.Stream;
import org.polypheny.prism.StreamFrame;

public class PrismUtils {


    public static StatementResponse createResult( PIStatement protoInterfaceStatement ) {
        return StatementResponse.newBuilder()
                .setStatementId( protoInterfaceStatement.getId() )
                .build();
    }


    public static StatementResponse createResult( PIStatement protoInterfaceStatement, StatementResult result ) {
        return StatementResponse.newBuilder()
                .setStatementId( protoInterfaceStatement.getId() )
                .setResult( result )
                .build();
    }


    public static StatementBatchResponse createStatementBatchStatus( int batchId ) {
        return StatementBatchResponse.newBuilder()
                .setBatchId( batchId )
                .build();
    }


    public static StatementBatchResponse createStatementBatchStatus( int batchId, List<Long> updateCounts ) {
        return StatementBatchResponse.newBuilder()
                .setBatchId( batchId )
                .addAllScalars( updateCounts )
                .build();
    }


    public static PreparedStatementSignature createPreparedStatementSignature( PIPreparedStatement preparedStatement ) {
        return PreparedStatementSignature.newBuilder()
                .setStatementId( preparedStatement.getId() )
                .addAllParameterMetas( preparedStatement.getParameterMetas() )
                .build();
    }


    public static Row serializeToRow( List<PolyValue> row, StreamingIndex streamingIndex ) {
        return Row.newBuilder()
                .addAllValues( PolyValueSerializer.serializeList( row, streamingIndex ) )
                .build();
    }


    public static List<Row> serializeToRows( List<List<PolyValue>> rows, StreamingIndex streamingIndex ) {
        return rows.stream().map(r -> PrismUtils.serializeToRow(r, streamingIndex) ).collect( Collectors.toList() );
    }


    public static List<ProtoNode> serializeToNodes( List<List<PolyValue>> data, StreamingIndex streamingIndex ) {
        return data.stream().map( e -> PolyValueSerializer.buildProtoNode( (PolyNode) (e.get( 0 )), streamingIndex ) ).collect( Collectors.toList() );
    }


    public static List<ProtoEdge> serializeToEdges( List<List<PolyValue>> data, StreamingIndex streamingIndex ) {
        return data.stream().map( e -> PolyValueSerializer.buildProtoEdge( (PolyEdge) (e.get( 0 )), streamingIndex ) ).collect( Collectors.toList() );
    }


    public static List<ProtoPath> serializeToPaths( List<List<PolyValue>> data, StreamingIndex streamingIndex ) {
        return data.stream().map( e -> PolyValueSerializer.buildProtoPath( (PolyPath) (e.get( 0 )), streamingIndex ) ).collect( Collectors.toList() );
    }

    public static StreamFrame buildStreamFrame(byte[] data, boolean is_last) {
        return StreamFrame.newBuilder()
                .setData( ByteString.copyFrom(data) )
                .setIsLast( is_last )
                .build();
    }


    public static Frame buildGraphFrame( boolean isLast, List<List<PolyValue>> data, StreamingIndex streamingIndex ) {
        GraphFrame.Builder graphFrameBuilder = GraphFrame.newBuilder();
        if ( !data.isEmpty() ) {
            PolyType elementType = data.get( 0 ).get( 0 ).getType();
            switch ( elementType ) {
                case NODE -> graphFrameBuilder.addAllNodes( serializeToNodes( data, streamingIndex ) );
                case EDGE -> graphFrameBuilder.addAllEdges( serializeToEdges( data, streamingIndex ) );
                case PATH -> graphFrameBuilder.addAllPaths( serializeToPaths( data, streamingIndex ) );
                default -> throw new RuntimeException( "Should never be thrown!" );
            }
        }

        return Frame.newBuilder()
                .setIsLast( isLast )
                .setGraphFrame( graphFrameBuilder.build() )
                .build();
    }


    public static ProtoPolyType getProtoFromPolyType( PolyType polyType ) {
        return ProtoPolyType.valueOf( polyType.getName() );
    }

}
