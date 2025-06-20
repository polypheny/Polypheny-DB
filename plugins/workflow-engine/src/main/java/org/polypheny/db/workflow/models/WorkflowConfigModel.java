/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.models;

import java.util.Map;
import lombok.Value;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.workflow.engine.monitoring.ExecutionInfo;
import org.polypheny.db.workflow.engine.storage.StorageManager;


@Value
public class WorkflowConfigModel {

    Map<DataModel, String> preferredStores;
    boolean fusionEnabled;
    boolean pipelineEnabled;
    int timeoutSeconds; // 0 for no timeout
    boolean dropUnusedCheckpoints;
    int maxWorkers;
    int pipelineQueueCapacity;
    int logCapacity;


    public static WorkflowConfigModel of() {
        return new WorkflowConfigModel(
                Map.of( DataModel.RELATIONAL, StorageManager.DEFAULT_CHECKPOINT_ADAPTER,
                        DataModel.DOCUMENT, StorageManager.DEFAULT_CHECKPOINT_ADAPTER,
                        DataModel.GRAPH, StorageManager.DEFAULT_CHECKPOINT_ADAPTER ),
                false,
                false,
                0,
                false,
                3,
                1000,
                100
        );
    }


    public void validate() throws GenericRuntimeException {
        if ( maxWorkers <= 0 ) {
            throw new GenericRuntimeException( "Max worker count must be greater than 0" );
        }
        if ( pipelineQueueCapacity <= 0 ) {
            throw new GenericRuntimeException( "Pipeline queue capacity must be greater than 0" );
        }
        if ( logCapacity < ExecutionInfo.MIN_LOG_CAPACITY ) {
            throw new GenericRuntimeException( "Log capacity must be greater than " + ExecutionInfo.MIN_LOG_CAPACITY );
        }
    }


    public WorkflowConfigModel withOptimizationsEnabled() {
        return new WorkflowConfigModel(
                preferredStores,
                true, true,
                timeoutSeconds,
                true,
                maxWorkers,
                pipelineQueueCapacity,
                logCapacity
        );
    }

}
