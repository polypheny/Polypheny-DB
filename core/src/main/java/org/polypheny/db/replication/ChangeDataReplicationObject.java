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

package org.polypheny.db.replication;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


/**
 * Targeted replication object that is created out of an ordinary ChangeDataCaptureObject.
 * Is directly constructed and targeted to specific placements and only needs to be converted to the specific modification.
 *
 * Objects can be directly used in DML Routing to lazily update outdated placements
 */
@Slf4j
public class ChangeDataReplicationObject {

    // TODO @HENNLO maybe introduce an Object per Operation DELETE_REPLICATION, UPDATE_REPLICATION or INSERT_REPLICATION


    @Getter
    private final long replicationDataId;

    @Getter
    private final long parentTxId;

    @Getter
    private final long tableId;

    @Getter
    private final Operation operation;

    @Getter
    private final ImmutableList<String> updateColumnList;
    @Getter
    private final ImmutableList<RexNode> sourceExpressionList;

    @Getter
    private ImmutableList<Map<Long, Object>> parameterValues; // List of ( ParameterIndex -> Value )

    @Getter
    private final long commitTimestamp;


    // replicationId (Pair : (adapterId+partitionId)=unique partitionPlacements)
    // Used to identify all target partitionPlacements that shall receive this update
    @Getter
    private Map<Long, Pair> dependentReplicationIds;


    public ChangeDataReplicationObject(
            long replicationDataId,
            long parentTxId,
            long tableId,
            Operation operation,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            List<Map<Long, Object>> parameterValues,
            long commitTimestamp,
            Map<Long, Pair> dependentReplicationIds ) {

        this.replicationDataId = replicationDataId;
        this.parentTxId = parentTxId;
        this.tableId = tableId;
        this.operation = operation;
        this.updateColumnList = ImmutableList.copyOf( updateColumnList );
        this.sourceExpressionList = ImmutableList.copyOf( sourceExpressionList );

        this.parameterValues = ImmutableList.copyOf( parameterValues );

        this.commitTimestamp = commitTimestamp;
        this.dependentReplicationIds = Map.copyOf( dependentReplicationIds );

    }


    /**
     *
     */
    public void removeSingleReplicationFromDependency( long replicationId ) {

        if ( dependentReplicationIds.containsKey( replicationId ) ) {
            dependentReplicationIds.remove( replicationId );
        } else {
            log.warn( "Replication with id {} was not part of the replication object. Either it was not part from the beginning or has already been removed.", replicationId );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Replication with id {} was removed from replication object.", replicationId );
        }
    }

}
