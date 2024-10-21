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

import java.util.List;
import org.polypheny.db.prisminterface.statements.PIPreparedStatement;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.DocumentFrame;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

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


    private static Row serializeToRow( List<PolyValue> row ) {
        return Row.newBuilder()
                .addAllValues( PolyValueSerializer.serializeList( row ) )
                .build();
    }


    public static List<Row> serializeToRows( List<List<PolyValue>> rows ) {
        return rows.stream().map( PrismUtils::serializeToRow ).toList();
    }


    private static List<GraphElement> serializeToGraphElement( List<List<PolyValue>> data ) {
        return data.stream().map( e -> PolyValueSerializer.buildProtoGraphElement( e.get( 0 ) ) ).toList();
    }


    public static Frame buildRelationalFrame( boolean isLast, List<List<PolyValue>> rows, List<ColumnMeta> metas ) {
        RelationalFrame relationalFrame = RelationalFrame.newBuilder()
                .addAllColumnMeta( metas )
                .addAllRows( serializeToRows( rows ) )
                .build();
        return Frame.newBuilder()
                .setIsLast( isLast )
                .setRelationalFrame( relationalFrame )
                .build();
    }


    public static Frame buildDocumentFrame( boolean isLast, List<PolyValue> data ) {
        List<ProtoDocument> documents = data.stream()
                .map( PolyValue::asDocument )
                .map( PolyValueSerializer::buildProtoDocument )
                .toList();
        DocumentFrame documentFrame = DocumentFrame.newBuilder()
                .addAllDocuments( documents )
                .build();
        return Frame.newBuilder()
                .setIsLast( isLast )
                .setDocumentFrame( documentFrame )
                .build();
    }


    public static Frame buildGraphFrame( boolean isLast, List<List<PolyValue>> data ) {
        GraphFrame.Builder graphFrameBuilder = GraphFrame.newBuilder();
        if ( !data.isEmpty() ) {
            graphFrameBuilder.addAllElement( serializeToGraphElement( data ) );
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
