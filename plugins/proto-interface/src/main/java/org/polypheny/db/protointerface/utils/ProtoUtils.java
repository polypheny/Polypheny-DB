/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.utils;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.RelationalFrame;
import org.polypheny.db.protointerface.proto.Row;
import org.polypheny.db.protointerface.proto.StatementBatchStatus;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.StatementStatus;
import org.polypheny.db.protointerface.statements.ParameterizedInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatement;
import org.polypheny.db.protointerface.statements.ProtoInterfaceStatementBatch;
import org.polypheny.db.protointerface.statements.Signaturizable;
import org.polypheny.db.type.entity.PolyValue;

public class ProtoUtils {


    public static StatementStatus createStatus( ProtoInterfaceStatement protoInterfaceStatement ) {
        return StatementStatus.newBuilder()
                .setStatementId( protoInterfaceStatement.getStatementId() )
                .build();
    }


    public static StatementStatus createStatus( ProtoInterfaceStatement protoInterfaceStatement, StatementResult result ) {
        return StatementStatus.newBuilder()
                .setStatementId( protoInterfaceStatement.getStatementId() )
                .setResult( result )
                .build();
    }


    public static StatementBatchStatus createStatementBatchStatus( ProtoInterfaceStatementBatch protoInterfaceStatementBatch ) {
        return StatementBatchStatus.newBuilder()
                .setBatchId( protoInterfaceStatementBatch.getBatchId() )
                .build();
    }


    public static StatementBatchStatus createStatementBatchStatus( ProtoInterfaceStatementBatch protoInterfaceStatementBatch, List<Long> updateCounts ) {
        return StatementBatchStatus.newBuilder()
                .setBatchId( protoInterfaceStatementBatch.getBatchId() )
                .addAllScalars( updateCounts )
                .build();
    }


    public static PreparedStatementSignature createPreparedStatementSignature( Signaturizable preparedStatement ) {
        return PreparedStatementSignature.newBuilder()
                .setStatementId( preparedStatement.getStatementId() )
                .addAllParameterMetas( preparedStatement.determineParameterMeta() )
                .build();
    }


    public static Row serializeToRow( List<PolyValue> row ) {
        return Row.newBuilder()
                .addAllValues( PolyValueSerializer.serializeList( row ) )
                .build();
    }


    public static List<Row> serializeToRows( List<List<PolyValue>> rows ) {
        return rows.stream().map( ProtoUtils::serializeToRow ).collect( Collectors.toList() );
    }


    public static Frame buildRelationalFrame( long offset, boolean isLast, List<List<PolyValue>> rows, List<ColumnMeta> metas ) {
        RelationalFrame relationalFrame = RelationalFrame.newBuilder()
                .addAllColumnMeta( metas )
                .addAllRows( serializeToRows( rows ) )
                .build();
        return Frame.newBuilder()
                .setIsLast( isLast )
                .setOffset( offset )
                .setRelationalFrame( relationalFrame )
                .build();
    }

}
