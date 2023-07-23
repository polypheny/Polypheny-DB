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

import org.polypheny.db.protointerface.proto.*;
import org.polypheny.db.protointerface.statements.PIPreparedStatement;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.type.entity.PolyValue;

import java.util.List;
import java.util.stream.Collectors;

public class ProtoUtils {


    public static StatementStatus createStatus(PIStatement protoInterfaceStatement) {
        return StatementStatus.newBuilder()
                .setStatementId(protoInterfaceStatement.getId())
                .build();
    }


    public static StatementStatus createStatus(PIStatement protoInterfaceStatement, StatementResult result) {
        return StatementStatus.newBuilder()
                .setStatementId(protoInterfaceStatement.getId())
                .setResult(result)
                .build();
    }


    public static StatementBatchStatus createStatementBatchStatus(int batchId) {
        return StatementBatchStatus.newBuilder()
                .setBatchId(batchId)
                .build();
    }


    public static StatementBatchStatus createStatementBatchStatus(int batchId, List<Long> updateCounts) {
        return StatementBatchStatus.newBuilder()
                .setBatchId(batchId)
                .addAllScalars(updateCounts)
                .build();
    }


    public static PreparedStatementSignature createPreparedStatementSignature(PIPreparedStatement preparedStatement) {
        return PreparedStatementSignature.newBuilder()
                .setStatementId(preparedStatement.getId())
                .addAllParameterMetas(preparedStatement.getParameterMetas())
                .build();
    }


    public static Row serializeToRow(List<PolyValue> row) {
        return Row.newBuilder()
                .addAllValues(PolyValueSerializer.serializeList(row))
                .build();
    }


    public static List<Row> serializeToRows(List<List<PolyValue>> rows) {
        return rows.stream().map(ProtoUtils::serializeToRow).collect(Collectors.toList());
    }


    public static Frame buildRelationalFrame(boolean isLast, List<List<PolyValue>> rows, List<ColumnMeta> metas) {
        RelationalFrame relationalFrame = RelationalFrame.newBuilder()
                .addAllColumnMeta(metas)
                .addAllRows(serializeToRows(rows))
                .build();
        return Frame.newBuilder()
                .setIsLast(isLast)
                .setRelationalFrame(relationalFrame)
                .build();
    }
}
