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


import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.util.Pair;


public class InsertReplicationObject extends ChangeDataReplicationObject {

    public InsertReplicationObject(
            long replicationDataId,
            long parentTxId,
            long tableId,
            Operation operation,
            List<AlgDataTypeField> fieldList,
            Map<Long, AlgDataType> parameterTypes,
            List<Map<Long, Object>> parameterValues,
            long commitTimestamp,
            Map<Long, Pair> dependentReplicationIds ) {

        super( replicationDataId, parentTxId, tableId, operation, fieldList, parameterTypes, parameterValues, commitTimestamp, dependentReplicationIds );
    }
}
