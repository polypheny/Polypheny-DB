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


import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.logical.LogicalModifyDataCapture;
import org.polypheny.db.transaction.Statement;


public class LogicalDataCaptureShuttle extends AlgShuttleImpl {

    @Getter
    private ChangeDataCaptureObject cdcObject;
    private final Statement statement;


    public LogicalDataCaptureShuttle( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public AlgNode visit( LogicalModifyDataCapture dataCapture ) {
        super.visit( dataCapture );

        cdcObject = ChangeDataCaptureObject.create(
                statement.getTransaction().getId(),
                statement.getId(),
                dataCapture.getTableId(),
                dataCapture.getOperation(),
                dataCapture.getUpdateColumnList(),
                dataCapture.getSourceExpressionList(),
                dataCapture.getCondition(),
                dataCapture.getFieldList(),
                dataCapture.getAccessedPartitions()
        );

        return dataCapture;
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        return visitChildren( other );
    }
}
