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

package org.polypheny.db.replication.cdc;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexNode;


/**
 * General purpose object that is enriched with all coarse grain information.
 * Necessary to lazily replicate captured changes.
 */
public class ChangeDataCaptureObject {


    @Getter
    private final long parentTxId;

    @Getter
    private final long stmtId;

    @Getter
    private final long tableId;

    @Getter
    private final Operation operation;

    @Getter
    private final ImmutableList<String> updateColumnList;
    @Getter
    private final ImmutableList<RexNode> sourceExpressionList;

    @Getter
    private final RexNode condition;

    @Getter
    private final List<AlgDataTypeField> fieldList;

    @Getter
    private final ImmutableList<Long> accessedPartitions;

    @Getter
    @Setter
    private Map<Long, AlgDataType> parameterTypes; // List of ( ParameterIndex -> Value )

    @Getter
    @Setter
    private List<Map<Long, Object>> parameterValues; // List of ( ParameterIndex -> Value )


    public ChangeDataCaptureObject(
            long parentTxId,
            long stmtId,
            long tableId,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions ) {

        this.parentTxId = parentTxId;
        this.stmtId = stmtId;
        this.tableId = tableId;
        this.operation = operation;

        if ( updateColumnList != null ) {
            this.updateColumnList = ImmutableList.copyOf( updateColumnList );
        } else {
            this.updateColumnList = ImmutableList.copyOf( new ArrayList<>() );
        }

        if ( sourceExpressionList != null ) {
            this.sourceExpressionList = ImmutableList.copyOf( sourceExpressionList );
        } else {
            this.sourceExpressionList = ImmutableList.copyOf( new ArrayList<>() );
        }

        this.fieldList = ImmutableList.copyOf( fieldList );
        this.condition = condition;

        this.accessedPartitions = ImmutableList.copyOf( accessedPartitions );

    }


    public static ChangeDataCaptureObject create(
            long parentTxId,
            long stmtId,
            long tableId,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions ) {
        return new ChangeDataCaptureObject( parentTxId, stmtId, tableId, operation, updateColumnList, sourceExpressionList, condition, fieldList, accessedPartitions );
    }

}
